import CasarsMacCore
import SwiftUI

/// Shared block layout for notebook prose and genuine assistant messages.
/// Source editing and persistence remain with their existing owners.
struct NotebookMarkdownView: View {
    let blocks: [NotebookMarkdownPresentation.Block]

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            ForEach(blocks) { block in
                MarkdownBlockView(block: block)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        // A lone paragraph keeps the existing editable text value. Multiple
        // blocks need a container so the first Text does not inherit its frame.
        .accessibilityElement(children:
            blocks.count == 1 && blocks[0].kind == .paragraph ? .combine : .contain
        )
    }
}

private struct MarkdownBlockView: View {
    let block: NotebookMarkdownPresentation.Block

    var body: some View {
        switch block.kind {
        case .table(let columns):
            table(columns: columns)
        case .orderedList, .unorderedList:
            VStack(alignment: .leading, spacing: 6) {
                ForEach(block.children) { item in
                    HStack(alignment: .top, spacing: 8) {
                        Text(listMarker(item))
                        NotebookMarkdownView(blocks: item.children)
                    }
                }
            }
        case .blockQuote:
            HStack(alignment: .top, spacing: 10) {
                Rectangle().fill(.secondary.opacity(0.4)).frame(width: 3)
                NotebookMarkdownView(blocks: block.children)
            }
            .fixedSize(horizontal: false, vertical: true)
        case .header(let level):
            Text(block.text)
                .font(level == 1 ? .title : level == 2 ? .title2 : .headline)
                .fontWeight(.semibold)
                .fixedSize(horizontal: false, vertical: true)
        case .codeBlock:
            ScrollView(.horizontal) {
                Text(block.text).font(.system(.body, design: .monospaced))
                    .fixedSize(horizontal: true, vertical: true)
                    .padding(8)
            }
            .background(.quaternary.opacity(0.35), in: RoundedRectangle(cornerRadius: 5))
        case .thematicBreak:
            Divider()
        default:
            if block.children.isEmpty {
                Text(block.text).fixedSize(horizontal: false, vertical: true)
            } else {
                NotebookMarkdownView(blocks: block.children)
            }
        }
    }

    private func listMarker(_ item: NotebookMarkdownPresentation.Block) -> String {
        if block.kind == .orderedList, case .listItem(let ordinal) = item.kind {
            return "\(ordinal)."
        }
        return "•"
    }

    private func table(columns: [PresentationIntent.TableColumn]) -> some View {
        ScrollView(.horizontal) {
            Grid(alignment: .topLeading, horizontalSpacing: 0, verticalSpacing: 0) {
                ForEach(block.children) { row in
                    GridRow(alignment: .top) {
                        ForEach(columns.indices, id: \.self) { index in
                            let cell = row.children.first { $0.kind == .tableCell(columnIndex: index) }
                            Text(cell?.text ?? AttributedString())
                                .fontWeight(row.kind == .tableHeaderRow ? .semibold : .regular)
                                .fixedSize(horizontal: true, vertical: true)
                                .padding(.horizontal, 10)
                                .padding(.vertical, 7)
                                .gridColumnAlignment(alignment(columns[index].alignment))
                        }
                    }
                    .background(row.kind == .tableHeaderRow ? Color.secondary.opacity(0.14) : .clear)
                }
            }
            .overlay(RoundedRectangle(cornerRadius: 4).stroke(.secondary.opacity(0.25)))
            .padding(1)
        }
    }

    private func alignment(_ value: PresentationIntent.TableColumn.Alignment) -> HorizontalAlignment {
        switch value {
        case .left: .leading
        case .center: .center
        case .right: .trailing
        @unknown default: .leading
        }
    }
}
