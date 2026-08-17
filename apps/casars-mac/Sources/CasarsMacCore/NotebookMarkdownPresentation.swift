import Foundation

/// Display-only projection used by rich notebook and assistant surfaces.
///
/// The persisted source remains ordinary Markdown. CASA-RS control comments are
/// deliberately removed only from this projection so Rich mode never exposes
/// implementation metadata while Raw mode remains a lossless editor.
package enum NotebookMarkdownPresentation {
    package static func displaySource(_ source: String) -> String {
        let range = NSRange(source.startIndex..., in: source)
        let withoutControlComments = controlCommentPattern.stringByReplacingMatches(
            in: source,
            range: range,
            withTemplate: ""
        )
        return withoutControlComments.trimmingCharacters(in: .whitespacesAndNewlines)
    }

    package static func attributedString(_ source: String) -> AttributedString? {
        let displayed = displaySource(source)
        guard !displayed.isEmpty else { return nil }
        guard let parsed = try? AttributedString(
            markdown: displayed,
            options: AttributedString.MarkdownParsingOptions(interpretedSyntax: .full)
        ) else {
            return AttributedString(displayed)
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

    private static let controlCommentPattern = try! NSRegularExpression(
        pattern: #"<!--[\s\S]*?-->"#
    )
}
