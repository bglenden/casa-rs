import Foundation

/// The Rust-owned location and identity of one managed Markdown cell.
///
/// Offsets are UTF-8 byte offsets into `source`. They are intentionally kept
/// separate from Swift `String.Index` values so editing can never select a
/// different occurrence of duplicated cell text.
package struct NotebookManagedCellSpan: Identifiable, Equatable {
    package let id: String
    package let kind: String
    package let taskIntent: NotebookTaskIntent?
    package let fullStart: UInt64
    package let fullEnd: UInt64
    package let bodyStart: UInt64
    package let bodyEnd: UInt64

    package init(
        id: String,
        kind: String,
        taskIntent: NotebookTaskIntent? = nil,
        fullStart: UInt64,
        fullEnd: UInt64,
        bodyStart: UInt64,
        bodyEnd: UInt64
    ) {
        self.id = id
        self.kind = kind
        self.taskIntent = taskIntent
        self.fullStart = fullStart
        self.fullEnd = fullEnd
        self.bodyStart = bodyStart
        self.bodyEnd = bodyEnd
    }

    package func bodySource(in source: String) -> String? {
        guard let bodyStart = Int(exactly: bodyStart),
              let bodyEnd = Int(exactly: bodyEnd),
              bodyStart <= bodyEnd,
              bodyEnd <= source.utf8.count
        else { return nil }
        let lower = source.utf8.index(source.utf8.startIndex, offsetBy: bodyStart)
        let upper = source.utf8.index(source.utf8.startIndex, offsetBy: bodyEnd)
        guard let lower = String.Index(lower, within: source),
              let upper = String.Index(upper, within: source)
        else { return nil }
        return String(source[lower..<upper])
    }
}

package enum NotebookRichProjectionError: Error, Equatable, CustomStringConvertible {
    case emptyCellID(index: Int)
    case duplicateCellID(String)
    case invalidRange(cellID: String, range: String)
    case outOfBounds(cellID: String, range: String, sourceByteCount: Int)
    case nonUTF8Boundary(cellID: String, offset: UInt64)
    case overlappingFullRanges(cellID: String)
    case overlappingBodyRanges(cellID: String)

    package var description: String {
        switch self {
        case let .emptyCellID(index):
            "managed cell \(index + 1) has an empty ID"
        case let .duplicateCellID(id):
            "managed cell ID \(id) occurs more than once"
        case let .invalidRange(cellID, range):
            "managed cell \(cellID) has an invalid \(range) range"
        case let .outOfBounds(cellID, range, sourceByteCount):
            "managed cell \(cellID) has an out-of-bounds \(range) range for \(sourceByteCount) UTF-8 bytes"
        case let .nonUTF8Boundary(cellID, offset):
            "managed cell \(cellID) uses UTF-8 interior offset \(offset)"
        case let .overlappingFullRanges(cellID):
            "managed cell \(cellID) overlaps a previous full range"
        case let .overlappingBodyRanges(cellID):
            "managed cell \(cellID) overlaps a previous body range"
        }
    }
}

/// A normalized projection that is safe for the source-preserving Rich model.
/// Production callers construct this from a Rust `NotebookDocumentProjection`;
/// the only Markdown scanner is the explicitly named prototype adapter below.
package struct NotebookRichSourceProjection: Equatable {
    package let source: String
    package let cells: [NotebookManagedCellSpan]

    package init(source: String, cells: [NotebookManagedCellSpan]) throws {
        try Self.validate(source: source, cells: cells)
        self.source = source
        self.cells = cells
    }

    package init(projection: NotebookDocumentProjection) throws {
        try self.init(
            source: projection.source,
            cells: projection.cells.map {
                NotebookManagedCellSpan(
                    id: $0.id,
                    kind: $0.kind,
                    taskIntent: $0.taskIntent,
                    fullStart: $0.fullStart,
                    fullEnd: $0.fullEnd,
                    bodyStart: $0.bodyStart,
                    bodyEnd: $0.bodyEnd
                )
            }
        )
    }

    package static var empty: Self {
        // The empty source has no ranges to invalidate.
        try! Self(source: "", cells: [])
    }

    package func cell(id: String) -> NotebookManagedCellSpan? {
        cells.first { $0.id == id }
    }

    private static func validate(
        source: String,
        cells: [NotebookManagedCellSpan]
    ) throws {
        let sourceByteCount = source.utf8.count
        var seenIDs = Set<String>()
        var previousFullEnd = 0
        var previousBodyEnd = 0

        for (index, cell) in cells.enumerated() {
            guard !cell.id.isEmpty else {
                throw NotebookRichProjectionError.emptyCellID(index: index)
            }
            guard seenIDs.insert(cell.id).inserted else {
                throw NotebookRichProjectionError.duplicateCellID(cell.id)
            }

            let fullStart = try checkedInt(cell.fullStart, cellID: cell.id, range: "full")
            let fullEnd = try checkedInt(cell.fullEnd, cellID: cell.id, range: "full")
            let bodyStart = try checkedInt(cell.bodyStart, cellID: cell.id, range: "body")
            let bodyEnd = try checkedInt(cell.bodyEnd, cellID: cell.id, range: "body")

            guard fullStart < fullEnd else {
                throw NotebookRichProjectionError.invalidRange(cellID: cell.id, range: "full")
            }
            guard bodyStart <= bodyEnd else {
                throw NotebookRichProjectionError.invalidRange(cellID: cell.id, range: "body")
            }
            guard fullEnd <= sourceByteCount, bodyEnd <= sourceByteCount else {
                throw NotebookRichProjectionError.outOfBounds(
                    cellID: cell.id,
                    range: fullEnd > sourceByteCount ? "full" : "body",
                    sourceByteCount: sourceByteCount
                )
            }
            guard fullStart >= 0, bodyStart >= 0 else {
                throw NotebookRichProjectionError.outOfBounds(
                    cellID: cell.id,
                    range: fullStart < 0 || bodyStart < 0 ? "full" : "body",
                    sourceByteCount: sourceByteCount
                )
            }
            guard bodyStart >= fullStart, bodyEnd <= fullEnd else {
                throw NotebookRichProjectionError.invalidRange(cellID: cell.id, range: "body")
            }
            guard fullStart >= previousFullEnd else {
                throw NotebookRichProjectionError.overlappingFullRanges(cellID: cell.id)
            }
            guard bodyStart >= previousBodyEnd else {
                throw NotebookRichProjectionError.overlappingBodyRanges(cellID: cell.id)
            }

            for offset in [cell.fullStart, cell.fullEnd, cell.bodyStart, cell.bodyEnd] {
                guard isUTF8Boundary(source, offset: offset) else {
                    throw NotebookRichProjectionError.nonUTF8Boundary(
                        cellID: cell.id,
                        offset: offset
                    )
                }
            }

            previousFullEnd = fullEnd
            previousBodyEnd = bodyEnd
        }
    }

    private static func checkedInt(
        _ value: UInt64,
        cellID: String,
        range: String
    ) throws -> Int {
        guard let result = Int(exactly: value) else {
            throw NotebookRichProjectionError.outOfBounds(
                cellID: cellID,
                range: range,
                sourceByteCount: Int.max
            )
        }
        return result
    }

    private static func isUTF8Boundary(_ source: String, offset: UInt64) -> Bool {
        guard let index = Int(exactly: offset), index >= 0 else { return false }
        guard index < source.utf8.count else { return index == source.utf8.count }
        let byte = source.utf8[source.utf8.index(source.utf8.startIndex, offsetBy: index)]
        return (byte & 0xC0) != 0x80
    }
}

package enum NotebookRichBlockKind: Equatable {
    case rawProse
    case heading(level: Int)
    case managedCell
    case insertion
}

fileprivate enum NotebookRichBlockStorage: Equatable {
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

/// One source-preserving block in a normalized Rich notebook projection.
package struct NotebookRichBlock: Identifiable, Equatable {
    package let id: String
    package let kind: NotebookRichBlockKind
    package let managedCellID: String?
    fileprivate var storage: NotebookRichBlockStorage

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
            managedCellID == nil ? source : nil
        case let .heading(_, _, editable, _):
            editable
        case let .insertion(_, editable, _, _, _):
            editable
        }
    }

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
        kind: NotebookRichBlockKind,
        managedCellID: String? = nil,
        storage: NotebookRichBlockStorage
    ) {
        self.id = id
        self.kind = kind
        self.managedCellID = managedCellID
        self.storage = storage
    }

    fileprivate mutating func replaceEditableSource(with source: String) -> Bool {
        switch storage {
        case .literal:
            guard managedCellID == nil else { return false }
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
            leadingBoundary = String(repeating: newline, count: max(0, 2 - existingBreaks))
        }

        var trailingBoundary = ""
        if let rightContext {
            let existingBreaks = trailingLineBreakCount(editable)
                + leadingLineBreakCount(rightContext)
            trailingBoundary = String(repeating: newline, count: max(0, 2 - existingBreaks))
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
                    let carriageReturn = scalars.index(before: previous)
                    index = scalars[carriageReturn].value == 13 ? carriageReturn : previous
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

/// Source-preserving Rich model. Its only production input is a validated
/// Rust-backed `NotebookRichSourceProjection`.
package struct NotebookRichDocument: Equatable {
    package private(set) var blocks: [NotebookRichBlock]

    package init(projection: NotebookRichSourceProjection) {
        blocks = Self.makeBlocks(from: projection)
    }

    package static var empty: Self {
        Self(projection: .empty)
    }

    package var markdown: String {
        blocks.map(\.source).joined()
    }

    @discardableResult
    package mutating func replaceEditableSource(
        blockID: String,
        with source: String
    ) -> Bool {
        guard let index = blocks.firstIndex(where: { $0.id == blockID }) else { return false }
        let leftManagedSource = index > blocks.startIndex && blocks[index - 1].managedCellID != nil
            ? blocks[index - 1].source
            : nil
        let rightManagedSource = index < blocks.index(before: blocks.endIndex)
            && blocks[index + 1].managedCellID != nil
            ? blocks[index + 1].source
            : nil
        let boundedSource = Self.sourcePreservingManagedCellBoundaries(
            source,
            leftManagedSource: leftManagedSource,
            rightManagedSource: rightManagedSource
        )
        return blocks[index].replaceEditableSource(with: boundedSource)
    }

    private static func sourcePreservingManagedCellBoundaries(
        _ source: String,
        leftManagedSource: String?,
        rightManagedSource: String?
    ) -> String {
        let newline = preferredNewline(in: source)
        let leadingBreaks = leftManagedSource.map {
            NotebookRichBlock.trailingLineBreakCount($0)
                + NotebookRichBlock.leadingLineBreakCount(source)
        } ?? 2
        let trailingBreaks = rightManagedSource.map {
            NotebookRichBlock.trailingLineBreakCount(source)
                + NotebookRichBlock.leadingLineBreakCount($0)
        } ?? 2
        return String(repeating: newline, count: max(0, 2 - leadingBreaks))
            + source
            + String(repeating: newline, count: max(0, 2 - trailingBreaks))
    }

    private static func makeBlocks(from projection: NotebookRichSourceProjection) -> [NotebookRichBlock] {
        let newline = preferredNewline(in: projection.source)
        var result: [NotebookRichBlock] = []
        var nextID = 0
        var cursor = projection.source.startIndex
        var leftManagedSource: String?

        for cell in projection.cells {
            let fullRange = stringRange(
                in: projection.source,
                start: cell.fullStart,
                end: cell.fullEnd
            )!
            let managedSource = String(projection.source[fullRange])
            appendProseGap(
                String(projection.source[cursor..<fullRange.lowerBound]),
                leftContext: leftManagedSource,
                rightContext: managedSource,
                newline: newline,
                nextID: &nextID,
                to: &result
            )
            result.append(
                NotebookRichBlock(
                    id: blockID(nextID),
                    kind: .managedCell,
                    managedCellID: cell.id,
                    storage: .literal(managedSource)
                )
            )
            nextID += 1
            leftManagedSource = managedSource
            cursor = fullRange.upperBound
        }

        appendProseGap(
            String(projection.source[cursor..<projection.source.endIndex]),
            leftContext: leftManagedSource,
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
        to result: inout [NotebookRichBlock]
    ) {
        if source.allSatisfy(\.isWhitespace) {
            result.append(
                NotebookRichBlock(
                    id: blockID(nextID),
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
                NotebookRichBlock(
                    id: blockID(nextID),
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
                        NotebookRichBlock(
                            id: blockID(nextID),
                            kind: .rawProse,
                            storage: .literal(before)
                        )
                    )
                    nextID += 1
                }
            }

            result.append(
                NotebookRichBlock(
                    id: blockID(nextID),
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
                    NotebookRichBlock(
                        id: blockID(nextID),
                        kind: .rawProse,
                        storage: .literal(remainder)
                    )
                )
                nextID += 1
            }
        }
    }

    private static func blockID(_ sequence: Int) -> String {
        // Keep the reviewed accessibility identifier prefix stable.
        "rich-element-\(sequence)"
    }

    private static func preferredNewline(in source: String) -> String {
        if source.contains("\r\n") { return "\r\n" }
        if source.contains("\r") && !source.contains("\n") { return "\r" }
        return "\n"
    }

    private static func stringRange(
        in source: String,
        start: UInt64,
        end: UInt64
    ) -> Range<String.Index>? {
        guard let start = Int(exactly: start), let end = Int(exactly: end) else { return nil }
        guard let lower = stringIndex(in: source, utf8Offset: start),
              let upper = stringIndex(in: source, utf8Offset: end)
        else { return nil }
        return lower..<upper
    }

    private static func stringIndex(in source: String, utf8Offset: Int) -> String.Index? {
        guard utf8Offset >= 0, utf8Offset <= source.utf8.count else { return nil }
        let utf8Index = source.utf8.index(source.utf8.startIndex, offsetBy: utf8Offset)
        return String.Index(utf8Index, within: source)
    }

    private struct ExactLine {
        let range: Range<String.Index>
        let contentRange: Range<String.Index>
        let terminator: String
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
        let bytes = Array(source.utf8)
        var lineStart = 0
        var cursor = 0
        while cursor < bytes.count {
            while cursor < bytes.count, bytes[cursor] != 13, bytes[cursor] != 10 {
                cursor += 1
            }
            let contentEnd = cursor
            if cursor < bytes.count {
                if bytes[cursor] == 13 {
                    cursor += 1
                    if cursor < bytes.count, bytes[cursor] == 10 {
                        cursor += 1
                    }
                } else {
                    cursor += 1
                }
            }
            let lineLower = stringIndex(in: source, utf8Offset: lineStart)!
            let lineUpper = stringIndex(in: source, utf8Offset: cursor)!
            let contentUpper = stringIndex(in: source, utf8Offset: contentEnd)!
            result.append(
                ExactLine(
                    range: lineLower..<lineUpper,
                    contentRange: lineLower..<contentUpper,
                    terminator: String(source[contentUpper..<lineUpper])
                )
            )
            lineStart = cursor
        }
        return result
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
        guard indentation <= 3, index < line.endIndex, line[index] == "#" else { return nil }
        let hashesStart = index
        while index < line.endIndex, line[index] == "#" {
            index = line.index(after: index)
        }
        let level = line.distance(from: hashesStart, to: index)
        guard (1...6).contains(level), index < line.endIndex,
              line[index] == " " || line[index] == "\t"
        else { return nil }
        while index < line.endIndex, line[index] == " " || line[index] == "\t" {
            index = line.index(after: index)
        }
        return (level, String(line[..<index]), String(line[index...]))
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

/// Markdown scanner retained only for deterministic fixture/prototype input.
/// Production notebook projections must come from Rust byte ranges.
package enum PrototypeNotebookRichProjectionAdapter {
    package static func projection(markdown: String) throws -> NotebookRichSourceProjection {
        try NotebookRichSourceProjection(source: markdown, cells: managedCells(in: markdown))
    }

    package static func document(markdown: String) throws -> NotebookRichDocument {
        try NotebookRichDocument(projection: projection(markdown: markdown))
    }

    private struct ExactLine {
        let range: Range<String.Index>
        let contentRange: Range<String.Index>
    }

    private struct Fence {
        let marker: Character
        let length: Int
    }

    private static func managedCells(in source: String) -> [NotebookManagedCellSpan] {
        let lines = exactLines(in: source)
        var result: [NotebookManagedCellSpan] = []
        var activeFence: Fence?
        var index = 0

        while index < lines.count {
            let content = String(source[lines[index].contentRange])
            if let fence = activeFence {
                if closesFence(content, fence: fence) { activeFence = nil }
                index += 1
                continue
            }
            if let marker = managedMarker(from: content),
               let closingIndex = (index + 1..<lines.count).first(where: {
                   String(source[lines[$0].contentRange]).trimmingCharacters(in: .whitespaces)
                       == "<!-- /casa-rs-cell -->"
               })
            {
                let fullStart = utf8Offset(in: source, at: lines[index].range.lowerBound)
                let fullEnd = utf8Offset(in: source, at: lines[closingIndex].range.upperBound)
                let bodyStart = utf8Offset(in: source, at: lines[index].range.upperBound)
                let bodyEnd = utf8Offset(in: source, at: lines[closingIndex].range.lowerBound)
                result.append(
                    NotebookManagedCellSpan(
                        id: marker.id,
                        kind: marker.kind,
                        fullStart: UInt64(fullStart),
                        fullEnd: UInt64(fullEnd),
                        bodyStart: UInt64(bodyStart),
                        bodyEnd: UInt64(bodyEnd)
                    )
                )
                index = closingIndex + 1
                continue
            }
            if let fence = openingFence(content) { activeFence = fence }
            index += 1
        }
        return result
    }

    private static func utf8Offset(in source: String, at index: String.Index) -> Int {
        guard let utf8Index = index.samePosition(in: source.utf8) else { return 0 }
        return source.utf8.distance(from: source.utf8.startIndex, to: utf8Index)
    }

    private static func exactLines(in source: String) -> [ExactLine] {
        var result: [ExactLine] = []
        let bytes = Array(source.utf8)
        var lineStart = 0
        var cursor = 0
        while cursor < bytes.count {
            while cursor < bytes.count, bytes[cursor] != 13, bytes[cursor] != 10 {
                cursor += 1
            }
            let contentEndOffset = cursor
            if cursor < bytes.count {
                if bytes[cursor] == 13 {
                    cursor += 1
                    if cursor < bytes.count, bytes[cursor] == 10 {
                        cursor += 1
                    }
                } else {
                    cursor += 1
                }
            }
            let rangeStart = stringIndex(in: source, utf8Offset: lineStart)!
            let rangeEnd = stringIndex(in: source, utf8Offset: cursor)!
            let contentEnd = stringIndex(in: source, utf8Offset: contentEndOffset)!
            result.append(ExactLine(range: rangeStart..<rangeEnd, contentRange: rangeStart..<contentEnd))
            lineStart = cursor
        }
        return result
    }

    private static func stringIndex(in source: String, utf8Offset: Int) -> String.Index? {
        guard utf8Offset >= 0, utf8Offset <= source.utf8.count else { return nil }
        let utf8Index = source.utf8.index(source.utf8.startIndex, offsetBy: utf8Offset)
        return String.Index(utf8Index, within: source)
    }

    private static func managedMarker(from line: String) -> (id: String, kind: String)? {
        let indentation = line.prefix { $0 == " " }.count
        guard indentation <= 3, line.dropFirst(indentation).first != "\t" else { return nil }
        let trimmed = line.trimmingCharacters(in: .whitespaces)
        guard trimmed.hasPrefix("<!-- casa-rs-cell:v1 "), trimmed.hasSuffix("-->") else { return nil }
        let tokens = trimmed.split(whereSeparator: \.isWhitespace)
        guard let idToken = tokens.first(where: { $0.hasPrefix("id=") }),
              let kindToken = tokens.first(where: { $0.hasPrefix("kind=") })
        else { return nil }
        let id = String(idToken.dropFirst(3))
        let kind = String(kindToken.dropFirst(5))
        return id.isEmpty || kind.isEmpty ? nil : (id, kind)
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
        guard let candidate = openingFence(line), candidate.marker == fence.marker,
              candidate.length >= fence.length
        else { return false }
        return line.trimmingCharacters(in: .whitespaces).allSatisfy { $0 == fence.marker }
    }
}
