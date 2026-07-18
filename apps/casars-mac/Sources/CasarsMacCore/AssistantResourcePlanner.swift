// SPDX-License-Identifier: LGPL-3.0-or-later

import Foundation

/// Capacity reported by the active backend model. Units are deliberately
/// conservative: one UTF-8 byte consumes one capacity unit, so planning does
/// not depend on a provider tokenizer or a machine-tuned conversion factor.
package struct AssistantModelCapacity: Equatable {
    package var inputUnits: UInt64
    package var outputReserveUnits: UInt64
}

package struct AssistantResourceReservation: Equatable {
    package var id: String
    package var units: UInt64
}

package struct AssistantContextResourceRequest: Equatable {
    package var id: String
    package var desiredUnits: UInt64
    package var selected: Bool
    package var active: Bool
}

package struct AssistantResourcePlan: Equatable {
    package var contextUnits: [String: UInt64]
    package var corpusUnits: UInt64
    package var diagnostics: [String]

    package static func unavailable(_ diagnostic: String) -> Self {
        Self(contextUnits: [:], corpusUnits: 0, diagnostics: [diagnostic])
    }
}

package enum AssistantResourcePlannerError: Error, Equatable {
    case arithmeticOverflow(String)
    case reservesExceedCapacity(required: UInt64, available: UInt64)
}

/// Pure, backend-neutral planner for the prompt resources owned by CASA-RS.
/// Reserves are subtracted first. Each selected context receives one share,
/// the currently active context receives a second priority share, and corpus
/// retrieval receives one share. Input order deterministically owns remainder
/// units after active contexts are moved to the front.
package enum AssistantResourcePlanner {
    package static func plan(
        capacity: AssistantModelCapacity,
        reservations: [AssistantResourceReservation],
        contexts: [AssistantContextResourceRequest],
        corpusDesiredUnits: UInt64
    ) throws -> AssistantResourcePlan {
        let reserve = try reservations.reduce(capacity.outputReserveUnits) { partial, item in
            let (sum, overflow) = partial.addingReportingOverflow(item.units)
            guard !overflow else {
                throw AssistantResourcePlannerError.arithmeticOverflow("reservation \(item.id)")
            }
            return sum
        }
        guard reserve <= capacity.inputUnits else {
            throw AssistantResourcePlannerError.reservesExceedCapacity(
                required: reserve,
                available: capacity.inputUnits
            )
        }

        let eligible = contexts.enumerated()
            .filter { $0.element.selected && $0.element.desiredUnits > 0 }
            .sorted { lhs, rhs in
                if lhs.element.active != rhs.element.active { return lhs.element.active }
                return lhs.offset < rhs.offset
            }
            .map(\.element)
        let contextWeight = try eligible.reduce(0 as UInt64) { total, request in
            let (sum, overflow) = total.addingReportingOverflow(request.active ? 2 : 1)
            guard !overflow else {
                throw AssistantResourcePlannerError.arithmeticOverflow("context weights")
            }
            return sum
        }
        let (totalWeight, weightOverflow) = contextWeight.addingReportingOverflow(
            corpusDesiredUnits > 0 ? 1 : 0
        )
        guard !weightOverflow else {
            throw AssistantResourcePlannerError.arithmeticOverflow("consumer weights")
        }
        guard totalWeight > 0 else {
            return AssistantResourcePlan(contextUnits: [:], corpusUnits: 0, diagnostics: [])
        }
        let available = capacity.inputUnits - reserve
        let share = available / totalWeight
        var remainder = available % totalWeight
        var contextUnits: [String: UInt64] = [:]
        for request in eligible {
            let weight: UInt64 = request.active ? 2 : 1
            let bonus = min(remainder, weight)
            let (weightedShare, multiplyOverflow) = share.multipliedReportingOverflow(by: weight)
            let (offered, additionOverflow) = weightedShare.addingReportingOverflow(bonus)
            guard !multiplyOverflow, !additionOverflow else {
                throw AssistantResourcePlannerError.arithmeticOverflow("context \(request.id)")
            }
            remainder -= bonus
            contextUnits[request.id] = min(request.desiredUnits, offered)
        }
        let (corpusOffered, corpusOverflow) = share.addingReportingOverflow(
            remainder > 0 ? 1 : 0
        )
        guard !corpusOverflow else {
            throw AssistantResourcePlannerError.arithmeticOverflow("corpus allocation")
        }
        let corpusUnits = corpusDesiredUnits > 0 ? min(corpusDesiredUnits, corpusOffered) : 0
        return AssistantResourcePlan(
            contextUnits: contextUnits,
            corpusUnits: corpusUnits,
            diagnostics: []
        )
    }

    package static func truncate(_ value: String, unitLimit: UInt64) -> String {
        guard unitLimit > 0 else { return "" }
        guard UInt64(value.utf8.count) > unitLimit else { return value }
        let marker = "\n[… bounded by CASA-RS resource plan …]"
        let markerUnits = UInt64(marker.utf8.count)
        guard unitLimit > markerUnits else { return utf8Prefix(value, unitLimit: unitLimit) }
        return utf8Prefix(value, unitLimit: unitLimit - markerUnits) + marker
    }

    private static func utf8Prefix(_ value: String, unitLimit: UInt64) -> String {
        let count = Int(min(unitLimit, UInt64(Int.max)))
        var bytes = Array(value.utf8.prefix(count))
        while !bytes.isEmpty {
            if let prefix = String(bytes: bytes, encoding: .utf8) { return prefix }
            bytes.removeLast()
        }
        return ""
    }
}
