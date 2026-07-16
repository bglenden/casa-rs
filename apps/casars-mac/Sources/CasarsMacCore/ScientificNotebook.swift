import Foundation

/// Fixture-only prototype families exposed to the native app target.
///
/// These types are projections for interaction review. They are deliberately
/// prefixed with `Prototype`, are never written to disk, and are not notebook,
/// task-provider, or parameter-profile contracts.
package enum WorkbenchPrototypeKind: String, Codable, Equatable {
    case notebook
    case python
    case tutorial
    case ai
}

package enum NotebookPrototypeScenario: String, Codable, Equatable {
    case primary = "happy-path"
    case externalConflict = "external-conflict"
}

package enum PrototypeNotebookViewMode: String, CaseIterable, Codable, Equatable, Identifiable {
    case rich
    case raw

    package var id: String { rawValue }
}

package enum PrototypeNotebookReceiptStatus: String, Codable, Equatable {
    case running
    case succeeded
    case failed
    case cancelled
}

/// Read-only display value carried by the deterministic fixture adapter.
/// It is intentionally independent of provider parameter schemas.
package struct PrototypeNotebookParameterRow: Identifiable, Codable, Equatable {
    package var id: String { parameterID }
    package var parameterID: String
    package var label: String
    package var value: String

    package init(parameterID: String, label: String, value: String) {
        self.parameterID = parameterID
        self.label = label
        self.value = value
    }
}

package struct PrototypeNotebookExecutionRevision: Identifiable, Codable, Equatable {
    package let id: String
    package var sequence: Int
    package var timestamp: String
    package var status: PrototypeNotebookReceiptStatus
    package var summary: String
    package var products: [String]
    package var diagnostics: [String]
    package var logLines: [String]

    package init(
        id: String,
        sequence: Int,
        timestamp: String,
        status: PrototypeNotebookReceiptStatus,
        summary: String,
        products: [String] = [],
        diagnostics: [String] = [],
        logLines: [String] = []
    ) {
        self.id = id
        self.sequence = sequence
        self.timestamp = timestamp
        self.status = status
        self.summary = summary
        self.products = products
        self.diagnostics = diagnostics
        self.logLines = logLines
    }
}

/// Fixture projection for one inline task block in a prototype notebook.
package struct PrototypeNotebookTaskProjection: Identifiable, Codable, Equatable {
    package let id: String
    package var taskID: String
    package var title: String
    package var annotation: String
    package var contractVersion: UInt64
    package var sparseProfileTOML: String
    package var parameterRows: [PrototypeNotebookParameterRow]
    package var revisions: [PrototypeNotebookExecutionRevision]

    package init(
        id: String,
        taskID: String,
        title: String,
        annotation: String,
        contractVersion: UInt64,
        sparseProfileTOML: String,
        parameterRows: [PrototypeNotebookParameterRow],
        revisions: [PrototypeNotebookExecutionRevision]
    ) {
        self.id = id
        self.taskID = taskID
        self.title = title
        self.annotation = annotation
        self.contractVersion = contractVersion
        self.sparseProfileTOML = sparseProfileTOML
        self.parameterRows = parameterRows
        self.revisions = revisions
    }

    package var latestRevision: PrototypeNotebookExecutionRevision? {
        revisions.max { $0.sequence < $1.sequence }
    }
}

package struct PrototypeNotebookSummary: Identifiable, Codable, Equatable {
    package let id: String
    package var title: String
    package var filename: String
    package var displayPath: String

    package init(id: String, title: String, filename: String, displayPath: String) {
        self.id = id
        self.title = title
        self.filename = filename
        self.displayPath = displayPath
    }
}

/// Mutable in-memory projection for a single selectable prototype document.
package struct PrototypeNotebookDocumentProjection: Identifiable, Codable, Equatable {
    package let id: String
    package var title: String
    package var filename: String
    package var displayPath: String
    package var viewMode: PrototypeNotebookViewMode
    package var savedMarkdown: String
    package var draftMarkdown: String
    package var hasExternalConflict: Bool
    package var tasks: [PrototypeNotebookTaskProjection]
    package var selectedReceiptID: String?

    package init(
        id: String,
        title: String,
        filename: String,
        displayPath: String,
        viewMode: PrototypeNotebookViewMode = .rich,
        savedMarkdown: String,
        draftMarkdown: String,
        hasExternalConflict: Bool,
        tasks: [PrototypeNotebookTaskProjection],
        selectedReceiptID: String? = nil
    ) {
        self.id = id
        self.title = title
        self.filename = filename
        self.displayPath = displayPath
        self.viewMode = viewMode
        self.savedMarkdown = savedMarkdown
        self.draftMarkdown = draftMarkdown
        self.hasExternalConflict = hasExternalConflict
        self.tasks = tasks
        self.selectedReceiptID = selectedReceiptID
    }

    package var summary: PrototypeNotebookSummary {
        PrototypeNotebookSummary(id: id, title: title, filename: filename, displayPath: displayPath)
    }

    package var isDirty: Bool { draftMarkdown != savedMarkdown }

    package var selectedReceipt: PrototypeNotebookTaskProjection? {
        guard let selectedReceiptID else { return nil }
        return tasks.first { $0.id == selectedReceiptID }
    }
}

/// Aggregate fixture projection used only by the package-scoped prototype path.
package struct PrototypeScientificNotebookProjection: Codable, Equatable {
    package var prototypeKind: WorkbenchPrototypeKind
    package var scenario: NotebookPrototypeScenario
    package var documents: [PrototypeNotebookDocumentProjection]
    package var activeNotebookID: String
    package var nextSimulatedRunSequence: Int

    package init(
        prototypeKind: WorkbenchPrototypeKind = .notebook,
        scenario: NotebookPrototypeScenario,
        documents: [PrototypeNotebookDocumentProjection],
        activeNotebookID: String,
        nextSimulatedRunSequence: Int = 1
    ) {
        self.prototypeKind = prototypeKind
        self.scenario = scenario
        self.documents = documents
        self.activeNotebookID = activeNotebookID
        self.nextSimulatedRunSequence = nextSimulatedRunSequence
    }

    package var notebooks: [PrototypeNotebookSummary] { documents.map(\.summary) }

    package var activeDocument: PrototypeNotebookDocumentProjection? {
        documents.first { $0.id == activeNotebookID }
    }

    package var notebookID: String { activeDocument?.id ?? "" }
    package var title: String { activeDocument?.title ?? "" }
    package var filename: String { activeDocument?.filename ?? "" }
    package var displayPath: String { activeDocument?.displayPath ?? "" }
    package var viewMode: PrototypeNotebookViewMode { activeDocument?.viewMode ?? .rich }
    package var savedMarkdown: String { activeDocument?.savedMarkdown ?? "" }
    package var draftMarkdown: String { activeDocument?.draftMarkdown ?? "" }
    package var hasExternalConflict: Bool { activeDocument?.hasExternalConflict ?? false }
    package var receipts: [PrototypeNotebookTaskProjection] { activeDocument?.tasks ?? [] }
    package var selectedReceiptID: String? { activeDocument?.selectedReceiptID }
    package var selectedReceipt: PrototypeNotebookTaskProjection? { activeDocument?.selectedReceipt }
    package var isDirty: Bool { activeDocument?.isDirty ?? false }

    package func task(receiptID: String) -> PrototypeNotebookTaskProjection? {
        documents.lazy.flatMap(\.tasks).first { $0.id == receiptID }
    }
}

/// One source-preserving projection presented by the bounded Rich notebook
/// editor. The projection is fixture-only and deliberately does not define a
/// persisted notebook contract.
package enum PrototypeNotebookRichElementKind: Equatable {
    case rawProse
    case heading(level: Int)
    case task(cellID: String)
    case insertion
}

fileprivate enum PrototypeNotebookRichElementStorage: Equatable {
    case literal(String)
    case heading(leading: String, marker: String, editable: String, trailing: String)
    case insertion(
        base: String,
        editable: String,
        leftContext: String?,
        rightContext: String?,
        newline: String
    )
}

/// An exact source fragment or an inert insertion surface in a Rich document.
///
/// `source` is always the fragment that participates in serialization. For an
/// insertion surface it remains the original whitespace verbatim until the
/// user supplies non-empty `editableSource`.
package struct PrototypeNotebookRichElement: Identifiable, Equatable {
    package let id: String
    package let kind: PrototypeNotebookRichElementKind
    fileprivate var storage: PrototypeNotebookRichElementStorage

    package var source: String {
        switch storage {
        case let .literal(source):
            source
        case let .heading(leading, marker, editable, trailing):
            leading + marker + editable + trailing
        case let .insertion(base, editable, leftContext, rightContext, newline):
            Self.renderInsertion(
                base: base,
                editable: editable,
                leftContext: leftContext,
                rightContext: rightContext,
                newline: newline
            )
        }
    }

    package var editableSource: String? {
        switch storage {
        case let .literal(source):
            kind.taskID == nil ? source : nil
        case let .heading(_, _, editable, _):
            editable
        case let .insertion(_, editable, _, _, _):
            editable
        }
    }

    package var taskID: String? { kind.taskID }

    package var headingLevel: Int? {
        guard case let .heading(level) = kind else { return nil }
        return level
    }

    package var isInsertionSurface: Bool {
        if case .insertion = kind { return true }
        return false
    }

    fileprivate init(
        id: String,
        kind: PrototypeNotebookRichElementKind,
        storage: PrototypeNotebookRichElementStorage
    ) {
        self.id = id
        self.kind = kind
        self.storage = storage
    }

    fileprivate mutating func replaceEditableSource(with source: String) -> Bool {
        switch storage {
        case .literal:
            guard kind.taskID == nil else { return false }
            storage = .literal(source)
        case let .heading(leading, marker, _, trailing):
            storage = .heading(
                leading: leading,
                marker: marker,
                editable: source,
                trailing: trailing
            )
        case let .insertion(base, _, leftContext, rightContext, newline):
            storage = .insertion(
                base: base,
                editable: source,
                leftContext: leftContext,
                rightContext: rightContext,
                newline: newline
            )
        }
        return true
    }

    fileprivate mutating func appendTrailingLiteral(_ source: String) {
        guard !source.isEmpty else { return }
        switch storage {
        case let .heading(leading, marker, editable, trailing):
            storage = .heading(
                leading: leading,
                marker: marker,
                editable: editable,
                trailing: trailing + source
            )
        case .literal, .insertion:
            break
        }
    }

    private static func renderInsertion(
        base: String,
        editable: String,
        leftContext: String?,
        rightContext: String?,
        newline: String
    ) -> String {
        guard !editable.isEmpty else { return base }

        var leadingBoundary = ""
        if let leftContext {
            let existingBreaks = trailingLineBreakCount(leftContext + base)
                + leadingLineBreakCount(editable)
            leadingBoundary = String(
                repeating: newline,
                count: max(0, 2 - existingBreaks)
            )
        }

        var trailingBoundary = ""
        if let rightContext {
            let existingBreaks = trailingLineBreakCount(editable)
                + leadingLineBreakCount(rightContext)
            trailingBoundary = String(
                repeating: newline,
                count: max(0, 2 - existingBreaks)
            )
        }

        return base + leadingBoundary + editable + trailingBoundary
    }

    fileprivate static func leadingLineBreakCount(_ source: String) -> Int {
        var count = 0
        let scalars = source.unicodeScalars
        var index = scalars.startIndex
        while index < scalars.endIndex {
            let scalar = scalars[index]
            if scalar.value == 13 {
                count += 1
                let next = scalars.index(after: index)
                if next < scalars.endIndex, scalars[next].value == 10 {
                    index = scalars.index(after: next)
                } else {
                    index = next
                }
            } else if scalar.value == 10 {
                count += 1
                index = scalars.index(after: index)
            } else if scalar.value == 32 || scalar.value == 9 {
                index = scalars.index(after: index)
            } else {
                break
            }
        }
        return count
    }

    fileprivate static func trailingLineBreakCount(_ source: String) -> Int {
        var count = 0
        let scalars = source.unicodeScalars
        var index = scalars.endIndex
        while index > scalars.startIndex {
            let previous = scalars.index(before: index)
            let scalar = scalars[previous]
            if scalar.value == 10 {
                count += 1
                if previous > scalars.startIndex {
                    let possibleCarriageReturn = scalars.index(before: previous)
                    index = scalars[possibleCarriageReturn].value == 13
                        ? possibleCarriageReturn
                        : previous
                } else {
                    index = previous
                }
            } else if scalar.value == 13 {
                count += 1
                index = previous
            } else if scalar.value == 32 || scalar.value == 9 {
                index = previous
            } else {
                break
            }
        }
        return count
    }
}

private extension PrototypeNotebookRichElementKind {
    var taskID: String? {
        guard case let .task(cellID) = self else { return nil }
        return cellID
    }
}

/// Source-preserving model for the prototype Rich editor.
///
/// Every input byte belongs to exactly one element. Rich edits replace only
/// the selected element's editable projection; task cells and all other
/// fragments are concatenated without normalization. Empty prose gaps around
/// task cells are represented by inert insertion elements so a no-op load and
/// serialize is byte-for-byte exact.
package struct PrototypeNotebookRichDocument: Equatable {
    package private(set) var elements: [PrototypeNotebookRichElement]

    package init(markdown: String) {
        elements = Self.parse(markdown)
    }

    package var markdown: String {
        elements.map(\.source).joined()
    }

    @discardableResult
    package mutating func replaceEditableSource(
        elementID: String,
        with source: String
    ) -> Bool {
        guard let index = elements.firstIndex(where: { $0.id == elementID }) else {
            return false
        }
        let leftTaskSource = index > elements.startIndex && elements[index - 1].taskID != nil
            ? elements[index - 1].source
            : nil
        let rightTaskSource = index < elements.index(before: elements.endIndex)
            && elements[index + 1].taskID != nil
            ? elements[index + 1].source
            : nil
        let boundedSource = Self.sourcePreservingTaskBoundaries(
            source,
            leftTaskSource: leftTaskSource,
            rightTaskSource: rightTaskSource
        )
        return elements[index].replaceEditableSource(with: boundedSource)
    }

    private static func sourcePreservingTaskBoundaries(
        _ source: String,
        leftTaskSource: String?,
        rightTaskSource: String?
    ) -> String {
        let newline = preferredNewline(in: source)
        let leadingBreaks = leftTaskSource.map {
            PrototypeNotebookRichElement.trailingLineBreakCount($0)
                + PrototypeNotebookRichElement.leadingLineBreakCount(source)
        } ?? 2
        let trailingBreaks = rightTaskSource.map {
            PrototypeNotebookRichElement.trailingLineBreakCount(source)
                + PrototypeNotebookRichElement.leadingLineBreakCount($0)
        } ?? 2
        return String(repeating: newline, count: max(0, 2 - leadingBreaks))
            + source
            + String(repeating: newline, count: max(0, 2 - trailingBreaks))
    }

    private static func parse(_ markdown: String) -> [PrototypeNotebookRichElement] {
        let taskCells = taskCellRanges(in: markdown)
        let newline = preferredNewline(in: markdown)
        var result: [PrototypeNotebookRichElement] = []
        var nextID = 0
        var cursor = markdown.startIndex
        var leftTaskSource: String?

        for taskCell in taskCells {
            let taskSource = String(markdown[taskCell.range])
            appendProseGap(
                String(markdown[cursor..<taskCell.range.lowerBound]),
                leftContext: leftTaskSource,
                rightContext: taskSource,
                newline: newline,
                nextID: &nextID,
                to: &result
            )
            result.append(
                PrototypeNotebookRichElement(
                    id: elementID(nextID),
                    kind: .task(cellID: taskCell.id),
                    storage: .literal(taskSource)
                )
            )
            nextID += 1
            leftTaskSource = taskSource
            cursor = taskCell.range.upperBound
        }

        appendProseGap(
            String(markdown[cursor..<markdown.endIndex]),
            leftContext: leftTaskSource,
            rightContext: nil,
            newline: newline,
            nextID: &nextID,
            to: &result
        )
        return result
    }

    private static func appendProseGap(
        _ source: String,
        leftContext: String?,
        rightContext: String?,
        newline: String,
        nextID: inout Int,
        to result: inout [PrototypeNotebookRichElement]
    ) {
        if source.allSatisfy(\.isWhitespace) {
            result.append(
                PrototypeNotebookRichElement(
                    id: elementID(nextID),
                    kind: .insertion,
                    storage: .insertion(
                        base: source,
                        editable: "",
                        leftContext: leftContext,
                        rightContext: rightContext,
                        newline: newline
                    )
                )
            )
            nextID += 1
            return
        }

        let headings = headingRanges(in: source)
        guard !headings.isEmpty else {
            result.append(
                PrototypeNotebookRichElement(
                    id: elementID(nextID),
                    kind: .rawProse,
                    storage: .literal(source)
                )
            )
            nextID += 1
            return
        }

        var cursor = source.startIndex
        var lastHeadingIndex: Int?
        for heading in headings {
            let before = String(source[cursor..<heading.range.lowerBound])
            var headingLeading = ""
            if !before.isEmpty {
                if before.allSatisfy(\.isWhitespace) {
                    headingLeading = before
                } else {
                    result.append(
                        PrototypeNotebookRichElement(
                            id: elementID(nextID),
                            kind: .rawProse,
                            storage: .literal(before)
                        )
                    )
                    nextID += 1
                }
            }

            result.append(
                PrototypeNotebookRichElement(
                    id: elementID(nextID),
                    kind: .heading(level: heading.level),
                    storage: .heading(
                        leading: headingLeading,
                        marker: heading.marker,
                        editable: heading.editable,
                        trailing: heading.terminator
                    )
                )
            )
            lastHeadingIndex = result.count - 1
            nextID += 1
            cursor = heading.range.upperBound
        }

        let remainder = String(source[cursor..<source.endIndex])
        if !remainder.isEmpty {
            if remainder.allSatisfy(\.isWhitespace), let lastHeadingIndex {
                result[lastHeadingIndex].appendTrailingLiteral(remainder)
            } else {
                result.append(
                    PrototypeNotebookRichElement(
                        id: elementID(nextID),
                        kind: .rawProse,
                        storage: .literal(remainder)
                    )
                )
                nextID += 1
            }
        }
    }

    private static func elementID(_ sequence: Int) -> String {
        "rich-element-\(sequence)"
    }

    private static func preferredNewline(in source: String) -> String {
        if source.contains("\r\n") { return "\r\n" }
        if source.contains("\r") && !source.contains("\n") { return "\r" }
        return "\n"
    }

    private struct ExactLine {
        let range: Range<String.Index>
        let contentRange: Range<String.Index>
        let terminator: String
    }

    private struct TaskCellRange {
        let id: String
        let range: Range<String.Index>
    }

    private struct HeadingRange {
        let level: Int
        let marker: String
        let editable: String
        let terminator: String
        let range: Range<String.Index>
    }

    private struct Fence {
        let marker: Character
        let length: Int
    }

    private static func exactLines(in source: String) -> [ExactLine] {
        var result: [ExactLine] = []
        let scalars = source.unicodeScalars
        var cursor = scalars.startIndex
        while cursor < scalars.endIndex {
            let lineStart = cursor
            while cursor < scalars.endIndex,
                  scalars[cursor].value != 13,
                  scalars[cursor].value != 10
            {
                cursor = scalars.index(after: cursor)
            }
            let contentEnd = cursor
            if cursor < scalars.endIndex {
                if scalars[cursor].value == 13 {
                    cursor = scalars.index(after: cursor)
                    if cursor < scalars.endIndex, scalars[cursor].value == 10 {
                        cursor = scalars.index(after: cursor)
                    }
                } else {
                    cursor = scalars.index(after: cursor)
                }
            }
            result.append(
                ExactLine(
                    range: lineStart..<cursor,
                    contentRange: lineStart..<contentEnd,
                    terminator: String(source[contentEnd..<cursor])
                )
            )
        }
        return result
    }

    private static func taskCellRanges(in source: String) -> [TaskCellRange] {
        let lines = exactLines(in: source)
        var result: [TaskCellRange] = []
        var activeFence: Fence?
        var inHTMLComment = false
        var index = 0

        while index < lines.count {
            let content = String(source[lines[index].contentRange])
            if let fence = activeFence {
                if closesFence(content, fence: fence) { activeFence = nil }
                index += 1
                continue
            }
            if inHTMLComment {
                if content.contains("-->") { inHTMLComment = false }
                index += 1
                continue
            }
            if let cellID = managedCellID(from: content),
               let closingIndex = (index + 1..<lines.count).first(where: {
                   String(source[lines[$0].contentRange])
                       .trimmingCharacters(in: .whitespaces) == "<!-- /casa-rs-cell -->"
               })
            {
                result.append(
                    TaskCellRange(
                        id: cellID,
                        range: lines[index].range.lowerBound..<lines[closingIndex].range.upperBound
                    )
                )
                index = closingIndex + 1
                continue
            }
            if let fence = openingFence(content) {
                activeFence = fence
            } else if let opening = content.range(of: "<!--"),
                      content[opening.upperBound...].range(of: "-->") == nil
            {
                inHTMLComment = true
            }
            index += 1
        }
        return result
    }

    private static func managedCellID(from line: String) -> String? {
        let indentation = line.prefix { $0 == " " }.count
        guard indentation <= 3,
              line.dropFirst(indentation).first != "\t"
        else { return nil }
        let trimmed = line.trimmingCharacters(in: .whitespaces)
        guard trimmed.hasPrefix("<!-- casa-rs-cell:v1 "),
              trimmed.hasSuffix("-->"),
              trimmed.split(whereSeparator: \.isWhitespace).contains(where: {
                  $0 == "kind=task" || $0 == "kind=python" || $0 == "kind=output"
              })
        else { return nil }
        guard let idToken = trimmed
            .split(whereSeparator: \.isWhitespace)
            .first(where: { $0.hasPrefix("id=") })
        else { return nil }
        let id = idToken.dropFirst(3)
        return id.isEmpty ? nil : String(id)
    }

    private static func headingRanges(in source: String) -> [HeadingRange] {
        let lines = exactLines(in: source)
        var result: [HeadingRange] = []
        var activeFence: Fence?
        var inHTMLComment = false

        for line in lines {
            let content = String(source[line.contentRange])
            if let fence = activeFence {
                if closesFence(content, fence: fence) { activeFence = nil }
                continue
            }
            if inHTMLComment {
                if content.contains("-->") { inHTMLComment = false }
                continue
            }
            if let fence = openingFence(content) {
                activeFence = fence
                continue
            }
            if let opening = content.range(of: "<!--") {
                if content[opening.upperBound...].range(of: "-->") == nil {
                    inHTMLComment = true
                }
                continue
            }
            guard let heading = headingParts(content) else { continue }
            result.append(
                HeadingRange(
                    level: heading.level,
                    marker: heading.marker,
                    editable: heading.editable,
                    terminator: line.terminator,
                    range: line.range
                )
            )
        }
        return result
    }

    private static func headingParts(_ line: String) -> (level: Int, marker: String, editable: String)? {
        var index = line.startIndex
        var indentation = 0
        while index < line.endIndex, line[index] == " ", indentation < 4 {
            indentation += 1
            index = line.index(after: index)
        }
        guard indentation <= 3, index < line.endIndex, line[index] == "#" else {
            return nil
        }
        let hashesStart = index
        while index < line.endIndex, line[index] == "#" {
            index = line.index(after: index)
        }
        let level = line.distance(from: hashesStart, to: index)
        guard (1...6).contains(level),
              index < line.endIndex,
              line[index] == " " || line[index] == "\t"
        else { return nil }
        while index < line.endIndex, line[index] == " " || line[index] == "\t" {
            index = line.index(after: index)
        }
        return (
            level,
            String(line[..<index]),
            String(line[index...])
        )
    }

    private static func openingFence(_ line: String) -> Fence? {
        var index = line.startIndex
        var indentation = 0
        while index < line.endIndex, line[index] == " ", indentation < 4 {
            indentation += 1
            index = line.index(after: index)
        }
        guard indentation <= 3, index < line.endIndex else { return nil }
        let marker = line[index]
        guard marker == "`" || marker == "~" else { return nil }
        let start = index
        while index < line.endIndex, line[index] == marker {
            index = line.index(after: index)
        }
        let length = line.distance(from: start, to: index)
        return length >= 3 ? Fence(marker: marker, length: length) : nil
    }

    private static func closesFence(_ line: String, fence: Fence) -> Bool {
        guard let candidate = openingFence(line),
              candidate.marker == fence.marker,
              candidate.length >= fence.length
        else { return false }
        let trimmed = line.trimmingCharacters(in: .whitespaces)
        return trimmed.allSatisfy { $0 == fence.marker }
    }
}

/// Deterministic fixture adapter for the Wave 1 interaction gate.
///
/// It performs no file, provider, schema, parameter, dataset, or task access.
package enum PrototypeScientificNotebookFixtureAdapter {
    package static func make(scenario: NotebookPrototypeScenario) -> PrototypeScientificNotebookProjection {
        let imagerProfile = """
        [casars]
        format = 1
        surface = "imager"
        kind = "task"
        contract = 1

        [parameters]
        vis = "data/twhya_calibrated.ms"
        imagename = "products/twhya-mfs"
        imsize = 1024
        cell = "1arcsec"
        niter = 1000
        """
        let imagerRows = [
            PrototypeNotebookParameterRow(parameterID: "vis", label: "MeasurementSet", value: "data/twhya_calibrated.ms"),
            PrototypeNotebookParameterRow(parameterID: "imagename", label: "Image name", value: "products/twhya-mfs"),
            PrototypeNotebookParameterRow(parameterID: "imsize", label: "Image size", value: "1024"),
            PrototypeNotebookParameterRow(parameterID: "cell", label: "Cell size", value: "1arcsec"),
            PrototypeNotebookParameterRow(parameterID: "niter", label: "Iterations", value: "1000"),
        ]
        let completed = PrototypeNotebookTaskProjection(
            id: "receipt-imager-mfs",
            taskID: "imager",
            title: "Create TW Hya MFS image",
            annotation: "Use the calibrated continuum data for a reproducible reference image.",
            contractVersion: 1,
            sparseProfileTOML: imagerProfile,
            parameterRows: imagerRows,
            revisions: [
                PrototypeNotebookExecutionRevision(
                    id: "execution-imager-mfs-1",
                    sequence: 1,
                    timestamp: "2026-07-10 10:14 MDT",
                    status: .succeeded,
                    summary: "Created the MFS image and associated weight products.",
                    products: ["products/twhya-mfs.image", "products/twhya-mfs.weight"],
                    diagnostics: ["Prototype receipt: no task was executed."],
                    logLines: [
                        "Resolved sparse imager parameters.",
                        "Simulated 1,000 clean iterations.",
                        "Registered two fixture products.",
                    ]
                )
            ]
        )
        let failed = PrototypeNotebookTaskProjection(
            id: "receipt-impbcor-failed",
            taskID: "impbcor",
            title: "Apply primary-beam correction",
            annotation: "Correct the reference image before measuring off-axis flux density.",
            contractVersion: 1,
            sparseProfileTOML: """
            [casars]
            format = 1
            surface = "impbcor"
            kind = "task"
            contract = 1

            [parameters]
            imagename = "products/twhya-mfs.image"
            pbimage = "products/twhya-mfs.pb"
            outfile = "products/twhya-mfs.pbcor"
            """,
            parameterRows: [
                PrototypeNotebookParameterRow(parameterID: "imagename", label: "Image", value: "products/twhya-mfs.image"),
                PrototypeNotebookParameterRow(parameterID: "pbimage", label: "Primary beam", value: "products/twhya-mfs.pb"),
                PrototypeNotebookParameterRow(parameterID: "outfile", label: "Output", value: "products/twhya-mfs.pbcor"),
            ],
            revisions: [
                PrototypeNotebookExecutionRevision(
                    id: "execution-impbcor-1",
                    sequence: 1,
                    timestamp: "2026-07-10 10:22 MDT",
                    status: .failed,
                    summary: "Primary-beam input was unavailable.",
                    diagnostics: ["Prototype failure state: products/twhya-mfs.pb was not staged."],
                    logLines: ["Validated request.", "Stopped before writing output."]
                )
            ]
        )
        let cancelled = PrototypeNotebookTaskProjection(
            id: "receipt-imager-cancelled",
            taskID: "imager",
            title: "Try alternate robust weighting",
            annotation: "Compare resolution and sensitivity after the reference image is accepted.",
            contractVersion: 1,
            sparseProfileTOML: imagerProfile + "\nrobust = -0.5",
            parameterRows: imagerRows + [
                PrototypeNotebookParameterRow(parameterID: "robust", label: "Robust", value: "-0.5")
            ],
            revisions: [
                PrototypeNotebookExecutionRevision(
                    id: "execution-imager-cancelled-1",
                    sequence: 1,
                    timestamp: "2026-07-10 10:30 MDT",
                    status: .cancelled,
                    summary: "User cancelled the trial before products were registered.",
                    diagnostics: ["Prototype cancellation state: no task was executed."],
                    logLines: ["Simulated run started.", "Cancellation acknowledged."]
                )
            ]
        )
        let analysisMarkdown = """
        # TW Hya reduction notes

        The calibrated continuum data look healthy. I will make a first MFS
        image, inspect the primary-beam coverage, and then compare weighting.

        ## Imaging intent

        Keep this first pass conservative so its products can serve as the
        reference for later experiments.

        Use the calibrated continuum data for a reproducible reference image.

        \(taskCell(id: completed.id, profileTOML: completed.sparseProfileTOML))

        Correct the reference image before measuring off-axis flux density.

        \(taskCell(id: failed.id, profileTOML: failed.sparseProfileTOML))

        Compare resolution and sensitivity after the reference image is accepted.

        \(taskCell(id: cancelled.id, profileTOML: cancelled.sparseProfileTOML))
        """
        let analysis = PrototypeNotebookDocumentProjection(
            id: "notebook-twhya-analysis",
            title: "TW Hya Analysis",
            filename: "Analysis.md",
            displayPath: "notebooks/Analysis.md",
            savedMarkdown: analysisMarkdown,
            draftMarkdown: scenario == .externalConflict
                ? analysisMarkdown + "\n\nLocal unsaved note: compare the robust-weighting runs."
                : analysisMarkdown,
            hasExternalConflict: scenario == .externalConflict,
            tasks: [completed, failed, cancelled],
            selectedReceiptID: completed.id
        )

        let listobs = PrototypeNotebookTaskProjection(
            id: "receipt-listobs-summary",
            taskID: "listobs",
            title: "Summarize the observation",
            annotation: "Capture the scan and antenna inventory next to the observing notes.",
            contractVersion: 1,
            sparseProfileTOML: """
            [casars]
            format = 1
            surface = "listobs"
            kind = "task"
            contract = 1

            [parameters]
            vis = "data/twhya_calibrated.ms"
            verbose = true
            """,
            parameterRows: [
                PrototypeNotebookParameterRow(parameterID: "vis", label: "MeasurementSet", value: "data/twhya_calibrated.ms"),
                PrototypeNotebookParameterRow(parameterID: "verbose", label: "Verbose", value: "true"),
            ],
            revisions: [
                PrototypeNotebookExecutionRevision(
                    id: "execution-listobs-1",
                    sequence: 1,
                    timestamp: "2026-07-10 09:48 MDT",
                    status: .succeeded,
                    summary: "Recorded 8 scans, 43 antennas, and 2 spectral windows.",
                    diagnostics: ["Prototype receipt: no MeasurementSet was opened."],
                    logLines: ["Read fixture observation summary.", "Rendered fixture scan inventory."]
                )
            ]
        )
        let observationMarkdown = """
        # Observation log

        The 2026-07-09 continuum execution completed under stable weather.
        Antennas DA42 and DV18 need a closer look before calibration.

        ## Next check

        Review the scan summary alongside the observer log before flagging.

        Capture the scan and antenna inventory next to the observing notes.

        \(taskCell(id: listobs.id, profileTOML: listobs.sparseProfileTOML))
        """
        let observationLog = PrototypeNotebookDocumentProjection(
            id: "notebook-twhya-observation-log",
            title: "TW Hya Observation Log",
            filename: "Observation Log.md",
            displayPath: "notebooks/Observation Log.md",
            savedMarkdown: observationMarkdown,
            draftMarkdown: observationMarkdown,
            hasExternalConflict: false,
            tasks: [listobs],
            selectedReceiptID: listobs.id
        )

        return PrototypeScientificNotebookProjection(
            scenario: scenario,
            documents: [analysis, observationLog],
            activeNotebookID: analysis.id
        )
    }

    private static func taskCell(id: String, profileTOML: String) -> String {
        """
        <!-- casa-rs-cell:v1 id=\(id) kind=task -->
        ```toml
        \(profileTOML)
        ```
        <!-- /casa-rs-cell -->
        """
    }

    /// Reprojects fixture task parameters from the same Markdown source edited
    /// in Raw mode. In production this mapping belongs to the Rust notebook and
    /// provider-contract layers; the prototype keeps only a bounded TOML parser.
    package static func synchronizeTaskCells(
        in document: inout PrototypeNotebookDocumentProjection
    ) {
        for index in document.tasks.indices {
            let taskID = document.tasks[index].id
            guard let profileTOML = taskProfileTOML(
                cellID: taskID,
                markdown: document.draftMarkdown
            ),
            let rows = parameterRows(
                profileTOML: profileTOML,
                existing: document.tasks[index].parameterRows
            )
            else { continue }
            document.tasks[index].sparseProfileTOML = profileTOML
            document.tasks[index].parameterRows = rows
        }
    }

    private static func taskProfileTOML(cellID: String, markdown: String) -> String? {
        let marker = "<!-- casa-rs-cell:v1 id=\(cellID) kind=task -->"
        guard let markerRange = markdown.range(of: marker) else { return nil }
        let afterMarker = markdown[markerRange.upperBound...]
        guard let fenceStart = afterMarker.range(of: "```toml"),
              let firstNewline = afterMarker[fenceStart.upperBound...].firstIndex(of: "\n")
        else { return nil }
        let afterFence = afterMarker.index(after: firstNewline)
        guard let fenceEnd = afterMarker[afterFence...].range(of: "\n```") else { return nil }
        return String(afterMarker[afterFence..<fenceEnd.lowerBound])
            .trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private static func parameterRows(
        profileTOML: String,
        existing: [PrototypeNotebookParameterRow]
    ) -> [PrototypeNotebookParameterRow]? {
        let labels = Dictionary(uniqueKeysWithValues: existing.map { ($0.parameterID, $0.label) })
        var foundParameters = false
        var inParameters = false
        var rows: [PrototypeNotebookParameterRow] = []
        for line in profileTOML.components(separatedBy: .newlines) {
            let trimmed = line.trimmingCharacters(in: .whitespacesAndNewlines)
            if trimmed == "[parameters]" {
                foundParameters = true
                inParameters = true
                continue
            }
            if trimmed.hasPrefix("[") {
                inParameters = false
                continue
            }
            guard inParameters,
                  !trimmed.isEmpty,
                  !trimmed.hasPrefix("#"),
                  let separator = trimmed.firstIndex(of: "=")
            else { continue }
            let parameterID = trimmed[..<separator]
                .trimmingCharacters(in: .whitespacesAndNewlines)
            var value = trimmed[trimmed.index(after: separator)...]
                .trimmingCharacters(in: .whitespacesAndNewlines)
            if value.count >= 2, value.first == "\"", value.last == "\"" {
                value.removeFirst()
                value.removeLast()
            }
            guard !parameterID.isEmpty else { continue }
            rows.append(
                PrototypeNotebookParameterRow(
                    parameterID: parameterID,
                    label: labels[parameterID] ?? parameterID,
                    value: value
                )
            )
        }
        return foundParameters ? rows : nil
    }
}
