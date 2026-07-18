// SPDX-License-Identifier: LGPL-3.0-or-later

import Foundation

/// Capacity reported by the active backend model. Units are deliberately
/// conservative: one UTF-8 byte in encoded JSON content consumes one capacity
/// unit, so planning does not depend on a provider tokenizer or a machine-tuned
/// conversion factor.
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
    case duplicateContextID(String)
    case reservesExceedCapacity(required: UInt64, available: UInt64)
}

/// Pure, backend-neutral planner for the prompt resources owned by CASA-RS.
/// Reserves are subtracted first. Each selected context receives one share,
/// the currently active context receives a second priority share, and corpus
/// retrieval receives one share. Input order deterministically owns remainder
/// units after active contexts are moved to the front.
package enum AssistantResourcePlanner {
    private enum ConsumerKind {
        case context(String)
        case corpus
    }

    private struct Consumer {
        var kind: ConsumerKind
        var demand: UInt64
        var weight: UInt64
        var allocation: UInt64 = 0
    }

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
        var contextIDs = Set<String>()
        var consumers: [Consumer] = try eligible.map { request in
            guard contextIDs.insert(request.id).inserted else {
                throw AssistantResourcePlannerError.duplicateContextID(request.id)
            }
            return Consumer(
                kind: .context(request.id),
                demand: request.desiredUnits,
                weight: request.active ? 2 : 1
            )
        }
        if corpusDesiredUnits > 0 {
            consumers.append(Consumer(kind: .corpus, demand: corpusDesiredUnits, weight: 1))
        }
        guard !consumers.isEmpty else {
            return AssistantResourcePlan(contextUnits: [:], corpusUnits: 0, diagnostics: [])
        }

        var available = capacity.inputUnits - reserve
        var active = Array(consumers.indices)
        while available > 0, !active.isEmpty {
            let totalWeight = try active.reduce(0 as UInt64) { total, index in
                let (sum, overflow) = total.addingReportingOverflow(consumers[index].weight)
                guard !overflow else {
                    throw AssistantResourcePlannerError.arithmeticOverflow("consumer weights")
                }
                return sum
            }
            let share = available / totalWeight
            let saturated = try active.filter { index in
                let (offered, overflow) = share.multipliedReportingOverflow(
                    by: consumers[index].weight
                )
                guard !overflow else {
                    throw AssistantResourcePlannerError.arithmeticOverflow("consumer share")
                }
                return consumers[index].demand - consumers[index].allocation <= offered
            }
            if !saturated.isEmpty {
                for index in saturated {
                    let remainingDemand = consumers[index].demand - consumers[index].allocation
                    consumers[index].allocation += remainingDemand
                    available -= remainingDemand
                }
                let saturatedSet = Set(saturated)
                active.removeAll { saturatedSet.contains($0) }
                continue
            }

            for index in active {
                let (increment, overflow) = share.multipliedReportingOverflow(
                    by: consumers[index].weight
                )
                guard !overflow else {
                    throw AssistantResourcePlannerError.arithmeticOverflow("consumer allocation")
                }
                consumers[index].allocation += increment
                available -= increment
            }
            guard available > 0 else { break }

            // The remainder is smaller than the active weight sum. Spend it in
            // stable priority order, with active contexts already sorted first.
            for index in active where available > 0 {
                let remainingDemand = consumers[index].demand - consumers[index].allocation
                let bonus = min(available, min(consumers[index].weight, remainingDemand))
                consumers[index].allocation += bonus
                available -= bonus
            }
            break
        }

        var contextUnits: [String: UInt64] = [:]
        var corpusUnits: UInt64 = 0
        for consumer in consumers {
            switch consumer.kind {
            case let .context(id): contextUnits[id] = consumer.allocation
            case .corpus: corpusUnits = consumer.allocation
            }
        }
        return AssistantResourcePlan(
            contextUnits: contextUnits,
            corpusUnits: corpusUnits,
            diagnostics: []
        )
    }

    package static func truncate(_ value: String, unitLimit: UInt64) -> String {
        guard unitLimit > 0 else { return "" }
        guard encodedStringUnits(value) > unitLimit else { return value }
        let marker = "\n[… bounded by CASA-RS resource plan …]"
        let markerUnits = encodedStringUnits(marker)
        guard unitLimit > markerUnits else { return encodedPrefix(value, unitLimit: unitLimit) }
        return encodedPrefix(value, unitLimit: unitLimit - markerUnits) + marker
    }

    package static func encodedStringUnits(_ value: String) -> UInt64 {
        guard let encoded = try? JSONEncoder().encode(value), encoded.count >= 2 else {
            return UInt64.max
        }
        return UInt64(encoded.count - 2)
    }

    private static func encodedPrefix(_ value: String, unitLimit: UInt64) -> String {
        let boundaries = Array(value.indices) + [value.endIndex]
        var low = 0
        var high = boundaries.count
        while low < high {
            let middle = low + (high - low) / 2
            let prefix = String(value[..<boundaries[middle]])
            if encodedStringUnits(prefix) <= unitLimit {
                low = middle + 1
            } else {
                high = middle
            }
        }
        guard low > 0 else { return "" }
        return String(value[..<boundaries[low - 1]])
    }
}
