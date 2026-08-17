import CasarsMacCore
import SwiftUI

/// The canonical notebook Rich renderer. It owns block traversal, the kind
/// switch, editing boundaries, unresolved managed-cell rendering, and stable
/// accessibility identifiers. Notebook surfaces supply only resolved-cell and
/// post-block decorations.
struct NotebookRichDocumentView: View {
    @Binding var document: NotebookRichDocument
    let onMarkdownChange: (String) -> Void
    let resolvedManagedCell: (String) -> AnyView?
    let afterBlock: (NotebookRichBlock) -> AnyView?

    var body: some View {
        VStack(alignment: .leading, spacing: 18) {
            ForEach(document.blocks) { block in
                blockView(block)
                if let decoration = afterBlock(block) {
                    decoration
                }
            }
        }
        .accessibilityElement(children: .contain)
        .accessibilityIdentifier("notebook.richDocument")
    }

    @ViewBuilder
    private func blockView(_ block: NotebookRichBlock) -> some View {
        switch block.kind {
        case .managedCell:
            if let managedCellID = block.managedCellID,
               let resolved = resolvedManagedCell(managedCellID)
            {
                resolved
            } else if let managedCellID = block.managedCellID {
                WorkbenchMarkdownText(source: NotebookVisibleMarkdown.source(block.source))
                    .workbenchFont(.caption)
                    .foregroundStyle(.secondary)
                    .textSelection(.enabled)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .accessibilityIdentifier("notebook.managedFallback.\(managedCellID)")
            }
        case .rawProse, .heading, .insertion:
            RichMarkdownBlockEditor(
                source: editableBinding(for: block.id),
                headingLevel: block.headingLevel,
                isInsertionSurface: block.isInsertionSurface,
                accessibilityID: "notebook.richElement.\(block.id)"
            )
        }
    }

    private func editableBinding(for blockID: String) -> Binding<String> {
        Binding(
            get: {
                document.blocks.first(where: { $0.id == blockID })?.editableSource ?? ""
            },
            set: { value in
                var updated = document
                guard updated.replaceEditableSource(blockID: blockID, with: value) else { return }
                document = updated
                onMarkdownChange(updated.markdown)
            }
        )
    }
}

struct NotebookRichStructuralErrorView: View {
    let message: String
    let onSwitchToRaw: () -> Void

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            Label("Rich view unavailable", systemImage: "exclamationmark.triangle.fill")
                .workbenchFont(.headline)
                .foregroundStyle(.orange)
            Text("CASA-RS could not validate the Rust notebook cell projection. Raw mode is available so the source remains visible and editable.")
                .workbenchFont(.subheadline)
            Text(message)
                .workbenchFont(.caption, design: .monospaced)
                .foregroundStyle(.secondary)
                .textSelection(.enabled)
            Button("Switch to Raw mode", action: onSwitchToRaw)
                .buttonStyle(.borderedProminent)
                .accessibilityIdentifier("notebook.richStructuralError.switchToRaw")
        }
        .padding(14)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(Color.orange.opacity(0.08))
        .overlay(RoundedRectangle(cornerRadius: 7).stroke(Color.orange.opacity(0.4)))
        .clipShape(RoundedRectangle(cornerRadius: 7))
        .accessibilityElement(children: .contain)
        .accessibilityIdentifier("notebook.richStructuralError")
    }
}
