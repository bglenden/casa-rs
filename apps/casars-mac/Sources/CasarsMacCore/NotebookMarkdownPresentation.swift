import Foundation

/// Display-only projection used by rich notebook and assistant surfaces.
///
/// The persisted source remains ordinary Markdown. CASA-RS control comments are
/// deliberately removed only from this projection so Rich mode never exposes
/// implementation metadata while Raw mode remains a lossless editor.
package enum NotebookMarkdownPresentation {
    /// A display block, retaining the parser's hierarchy and inline attributes.
    package struct Block: Identifiable, Equatable {
        package let id: Int
        package let kind: PresentationIntent.Kind
        package var text = AttributedString()
        package var children: [Block] = []

    }

    package static func blocks(_ source: String) -> [Block] {
        guard let text = attributedString(source) else { return [] }
        var nodes: [Int: Block] = [:]
        var children: [Int: [Int]] = [:]
        var roots: [Int] = []
        for run in text.runs {
            let path = run.presentationIntent?.components.reversed().map { $0 }
                ?? [PresentationIntent(.paragraph, identity: 0).components[0]]
            var parent: Int?
            for component in path {
                if nodes[component.identity] == nil {
                    nodes[component.identity] = Block(id: component.identity, kind: component.kind)
                    if let parent {
                        children[parent, default: []].append(component.identity)
                    } else {
                        roots.append(component.identity)
                    }
                }
                parent = component.identity
            }
            if let leaf = parent {
                var inline = AttributedString(text[run.range])
                // Text handles inline styles, but does not lay out block intents.
                inline.presentationIntent = nil
                nodes[leaf]?.text.append(inline)
            }
        }
        func block(_ id: Int) -> Block {
            var result = nodes[id]!
            result.children = (children[id] ?? []).map(block)
            return result
        }
        return roots.map(block)
    }

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
        return (try? AttributedString(
            markdown: displayed,
            options: AttributedString.MarkdownParsingOptions(interpretedSyntax: .full)
        )) ?? AttributedString(displayed)
    }

    private static let controlCommentPattern = try! NSRegularExpression(
        pattern: #"<!--[\s\S]*?-->"#
    )
}
