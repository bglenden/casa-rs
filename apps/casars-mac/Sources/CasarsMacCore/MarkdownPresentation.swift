import Foundation

/// Generic Markdown presentation shared by workbench text surfaces.
///
/// This layer intentionally has no notebook or CASA knowledge. Notebook
/// control-comment handling belongs to `NotebookVisibleMarkdown`.
package enum MarkdownPresentation {
    package static func attributedString(_ source: String) -> AttributedString? {
        let source = source.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !source.isEmpty else { return nil }
        guard let parsed = try? AttributedString(
            markdown: source,
            options: AttributedString.MarkdownParsingOptions(interpretedSyntax: .full)
        ) else {
            return AttributedString(source)
        }
        return materializingBlockStructure(in: parsed)
    }

    private static func materializingBlockStructure(in parsed: AttributedString) -> AttributedString {
        var rendered = AttributedString()
        var previousBlock: MarkdownBlock?

        for run in parsed.runs {
            let block = MarkdownBlock(intent: run.presentationIntent)
            if block.identity != previousBlock?.identity {
                if let previousBlock {
                    let separator = block.isNextItem(after: previousBlock) ? "\n" : "\n\n"
                    rendered.append(AttributedString(separator))
                }
                if block.listItemIdentity != previousBlock?.listItemIdentity,
                   let marker = block.listMarker
                {
                    rendered.append(AttributedString(marker))
                }
                previousBlock = block
            }
            rendered.append(AttributedString(parsed[run.range]))
        }

        return rendered
    }

    private struct MarkdownBlock {
        enum ListStyle {
            case ordered
            case unordered
        }

        let identity: Int?
        let listItemIdentity: Int?
        let listOrdinal: Int?
        let listStyle: ListStyle?
        let listContainerIdentities: [Int]
        let listDepth: Int

        init(intent: PresentationIntent?) {
            let components = Array(intent?.components ?? [])
            identity = components.first?.identity

            var itemIdentity: Int?
            var ordinal: Int?
            var style: ListStyle?
            var containerIdentities: [Int] = []
            var depth = 0
            for component in components {
                switch component.kind {
                case let .listItem(itemOrdinal):
                    if itemIdentity == nil {
                        itemIdentity = component.identity
                        ordinal = itemOrdinal
                    }
                    depth += 1
                case .orderedList:
                    if style == nil { style = .ordered }
                    containerIdentities.append(component.identity)
                case .unorderedList:
                    if style == nil { style = .unordered }
                    containerIdentities.append(component.identity)
                default:
                    break
                }
            }
            listItemIdentity = itemIdentity
            listOrdinal = ordinal
            listStyle = style
            listContainerIdentities = containerIdentities
            listDepth = depth
        }

        var listMarker: String? {
            guard listItemIdentity != nil, let listStyle else { return nil }
            let indentation = String(repeating: "  ", count: max(0, listDepth - 1))
            switch listStyle {
            case .ordered:
                return "\(indentation)\(listOrdinal ?? 1). "
            case .unordered:
                return "\(indentation)• "
            }
        }

        func isNextItem(after previous: MarkdownBlock) -> Bool {
            listItemIdentity != nil
                && listItemIdentity != previous.listItemIdentity
                && listContainerIdentities == previous.listContainerIdentities
        }
    }
}

/// Notebook-only visible Markdown projection. It removes complete CASA-RS
/// control comments outside fenced code while preserving ordinary HTML
/// comments and every fenced-code byte.
package enum NotebookVisibleMarkdown {
    package static func source(_ source: String) -> String {
        let fencedRanges = fencedCodeRanges(in: source)
        let pattern = try! NSRegularExpression(pattern: #"<!--[\s\S]*?-->"#)
        let fullRange = NSRange(source.startIndex..., in: source)
        var removals: [Range<String.Index>] = []
        for match in pattern.matches(in: source, range: fullRange) {
            guard let range = Range(match.range, in: source) else { continue }
            let comment = String(source[range])
            let trimmedComment = comment.trimmingCharacters(in: .whitespacesAndNewlines)
            guard trimmedComment.hasPrefix("<!-- casa-rs-")
                || trimmedComment.hasPrefix("<!-- /casa-rs-") else { continue }
            guard !fencedRanges.contains(where: { $0.overlaps(range) }) else { continue }
            removals.append(range)
        }

        var visible = source
        for range in removals.reversed() {
            visible.removeSubrange(range)
        }
        return visible.trimmingCharacters(in: .whitespacesAndNewlines)
    }

    package static func attributedString(_ source: String) -> AttributedString? {
        MarkdownPresentation.attributedString(NotebookVisibleMarkdown.source(source))
    }

    private struct Fence {
        let marker: Character
        let length: Int
    }

    private static func fencedCodeRanges(in source: String) -> [Range<String.Index>] {
        var ranges: [Range<String.Index>] = []
        var cursor = source.startIndex
        var activeFence: (fence: Fence, start: String.Index)?

        while cursor < source.endIndex {
            let lineStart = cursor
            while cursor < source.endIndex, source[cursor] != "\r", source[cursor] != "\n" {
                cursor = source.index(after: cursor)
            }
            let contentEnd = cursor
            if cursor < source.endIndex {
                if source[cursor] == "\r" {
                    cursor = source.index(after: cursor)
                    if cursor < source.endIndex, source[cursor] == "\n" {
                        cursor = source.index(after: cursor)
                    }
                } else {
                    cursor = source.index(after: cursor)
                }
            }
            let lineEnd = cursor
            let content = String(source[lineStart..<contentEnd])
            if let fence = activeFence {
                if closesFence(content, fence: fence.fence) {
                    ranges.append(fence.start..<lineEnd)
                    activeFence = nil
                }
            } else if let fence = openingFence(content) {
                activeFence = (fence, lineStart)
            }
        }

        if let activeFence {
            ranges.append(activeFence.start..<source.endIndex)
        }
        return ranges
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
