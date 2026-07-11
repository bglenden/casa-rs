import CasarsMacCore
import SwiftUI

struct PersistentScientificNotebookView: View {
    @ObservedObject var store: WorkbenchStore
    @State private var richDocument = PrototypeNotebookRichDocument(markdown: "")
    @State private var expandedCellIDs: Set<String> = []

    private var document: NotebookDocumentState? {
        store.state.scientificNotebooks?.activeNotebook
    }

    var body: some View {
        Group {
            if let document {
                VStack(spacing: 0) {
                    toolbar(document)
                    Divider()
                    ScrollView {
                        VStack(alignment: .leading, spacing: 0) {
                            if document.conflict != nil {
                                conflictBanner
                                    .padding(.bottom, 20)
                            }
                            notebookBody(document)
                                .padding(.bottom, 80)
                        }
                        .padding(.horizontal, 44)
                        .padding(.top, 30)
                        .frame(maxWidth: 920, alignment: .leading)
                        .frame(maxWidth: .infinity)
                    }
                    .background(Color(nsColor: .textBackgroundColor))
                    .accessibilityIdentifier("notebook.document.scroll")
                }
                .onAppear { loadRichDocument(document) }
                .onChange(of: document.id) { _ in
                    if let current = self.document { loadRichDocument(current) }
                }
                .onChange(of: document.viewMode) { mode in
                    if mode == .rich, let current = self.document { loadRichDocument(current) }
                }
            }
        }
        .sheet(item: Binding(
            get: { store.state.pendingNotebookTaskReplacement },
            set: { value in
                if value == nil { store.cancelNotebookTaskReplacement() }
            }
        )) { preview in
            NotebookTaskReplacementSheet(store: store, preview: preview)
        }
    }

    private func toolbar(_ document: NotebookDocumentState) -> some View {
        HStack(spacing: 12) {
            VStack(alignment: .leading, spacing: 2) {
                Text(document.title)
                    .workbenchFont(.title3, weight: .semibold)
                Text("notebooks/\(document.filename)")
                    .workbenchFont(.caption, design: .monospaced)
                    .foregroundStyle(.secondary)
            }
            Spacer()
            Picker("View", selection: Binding(
                get: { document.viewMode },
                set: { store.setScientificNotebookViewMode($0) }
            )) {
                Label("Rich", systemImage: "doc.richtext")
                    .accessibilityIdentifier("notebook.viewMode.rich")
                    .tag(NotebookDocumentViewMode.rich)
                Label("Raw", systemImage: "chevron.left.forwardslash.chevron.right")
                    .accessibilityIdentifier("notebook.viewMode.raw")
                    .tag(NotebookDocumentViewMode.raw)
            }
            .labelsHidden()
            .pickerStyle(.segmented)
            .frame(width: 150)
            .accessibilityIdentifier("notebook.viewMode")

            Button {
                store.saveScientificNotebook()
            } label: {
                Label("Save", systemImage: "square.and.arrow.down")
            }
            .disabled(!document.isDirty || document.conflict != nil)
            .keyboardShortcut("s", modifiers: [.command])
            .accessibilityIdentifier("notebook.save")

            Text(document.isDirty ? "Edited" : "Saved")
                .workbenchFont(.caption, weight: .semibold)
                .foregroundStyle(document.isDirty ? Color.orange : Color(nsColor: .labelColor))
                .accessibilityIdentifier("notebook.dirtyState")
                .accessibilityValue(document.isDirty ? "dirty" : "saved")
        }
        .padding(.horizontal, 18)
        .padding(.vertical, 11)
    }

    private var conflictBanner: some View {
        VStack(alignment: .leading, spacing: 9) {
            Label("External edit conflict", systemImage: "exclamationmark.triangle.fill")
                .workbenchFont(.headline)
                .foregroundStyle(.orange)
            Text("This Markdown file changed outside CASA-RS. Choose the source to keep before saving.")
                .workbenchFont(.subheadline)
            HStack {
                Button("Keep Local Draft") {
                    store.resolveScientificNotebookConflict(keepingDraft: true)
                }
                .accessibilityIdentifier("notebook.conflict.keepDraft")
                Button("Reload External Version") {
                    store.resolveScientificNotebookConflict(keepingDraft: false)
                }
                .accessibilityIdentifier("notebook.conflict.reloadExternal")
            }
        }
        .padding(13)
        .background(Color.orange.opacity(0.12))
        .overlay(RoundedRectangle(cornerRadius: 8).stroke(Color.orange.opacity(0.55)))
        .clipShape(RoundedRectangle(cornerRadius: 8))
    }

    @ViewBuilder
    private func notebookBody(_ document: NotebookDocumentState) -> some View {
        if document.viewMode == .raw {
            TextEditor(text: Binding(
                get: { self.document?.draftSource ?? "" },
                set: { store.setScientificNotebookDraft($0) }
            ))
            .font(.system(size: 13, design: .monospaced))
            .scrollContentBackground(.hidden)
            .padding(10)
            .frame(minHeight: 680)
            .overlay(RoundedRectangle(cornerRadius: 6).stroke(Color.secondary.opacity(0.24)))
            .accessibilityIdentifier("notebook.editor.raw")
        } else {
            VStack(alignment: .leading, spacing: 18) {
                ForEach(richDocument.elements) { element in
                    if let cellID = element.taskID {
                        taskCell(cellID: cellID, document: document, fallback: element.source)
                    } else {
                        RichMarkdownBlockEditor(
                            source: richBinding(element.id),
                            headingLevel: element.headingLevel,
                            isInsertionSurface: element.isInsertionSurface,
                            accessibilityID: "notebook.richElement.\(element.id)"
                        )
                    }
                }
            }
        }
    }

    @ViewBuilder
    private func taskCell(cellID: String, document: NotebookDocumentState, fallback: String) -> some View {
        let receipts = document.receipts.filter { $0.cellId == cellID }.sorted { $0.revision > $1.revision }
        if let intent = document.cells.first(where: { $0.id == cellID })?.taskIntent {
            VStack(alignment: .leading, spacing: 5) {
                Button {
                    store.openScientificNotebookTask(cellID: cellID)
                } label: {
                    VStack(alignment: .leading, spacing: 6) {
                        HStack {
                            Text("# \(intent.surface)")
                                .font(.system(size: 12, weight: .semibold, design: .monospaced))
                                .foregroundStyle(.secondary)
                            Spacer()
                            Image(systemName: "arrow.up.forward.app")
                                .foregroundStyle(.tertiary)
                        }
                        Text("[parameters]")
                            .font(.system(size: 12, design: .monospaced))
                            .foregroundStyle(.secondary)
                        ForEach(intent.parameters.sorted(by: { $0.key < $1.key }), id: \.key) { name, value in
                            Text("\(name) = \(value.tomlLiteral)")
                                .font(.system(size: 12, design: .monospaced))
                                .lineLimit(1)
                        }
                    }
                    .padding(.horizontal, 12)
                    .padding(.vertical, 10)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .background(Color.secondary.opacity(0.065))
                    .clipShape(RoundedRectangle(cornerRadius: 5))
                    .contentShape(Rectangle())
                }
                .buttonStyle(.plain)
                .accessibilityIdentifier("notebook.parameters.open.\(cellID)")

                if let latest = receipts.first {
                    Button {
                        if expandedCellIDs.contains(cellID) { expandedCellIDs.remove(cellID) }
                        else { expandedCellIDs.insert(cellID) }
                    } label: {
                        HStack(spacing: 5) {
                            Circle().fill(statusColor(latest.status)).frame(width: 6, height: 6)
                            Text("\(latest.status.capitalized) · r\(latest.revision)")
                                .workbenchFont(.caption2, weight: .semibold)
                                .foregroundStyle(.secondary)
                            Image(systemName: expandedCellIDs.contains(cellID) ? "chevron.up" : "chevron.down")
                                .workbenchFont(.caption2)
                                .foregroundStyle(.tertiary)
                        }
                    }
                    .buttonStyle(.plain)
                    .accessibilityIdentifier("notebook.executionStatus.\(cellID)")
                    if expandedCellIDs.contains(cellID) {
                        VStack(alignment: .leading, spacing: 5) {
                            if !latest.products.isEmpty {
                                Text(latest.products.map(\.path).joined(separator: " · "))
                                    .workbenchFont(.caption, design: .monospaced)
                            }
                            ForEach(latest.diagnostics, id: \.self) { diagnostic in
                                Text(diagnostic).workbenchFont(.caption)
                            }
                        }
                        .padding(.leading, 11)
                    }
                }
            }
        } else {
            Text(fallback)
                .font(.system(size: 12, design: .monospaced))
                .textSelection(.enabled)
        }
    }

    private func richBinding(_ elementID: String) -> Binding<String> {
        Binding(
            get: { richDocument.elements.first(where: { $0.id == elementID })?.editableSource ?? "" },
            set: { value in
                var updated = richDocument
                guard updated.replaceEditableSource(elementID: elementID, with: value) else { return }
                richDocument = updated
                store.setScientificNotebookDraft(updated.markdown)
            }
        )
    }

    private func loadRichDocument(_ document: NotebookDocumentState) {
        richDocument = PrototypeNotebookRichDocument(markdown: document.draftSource)
    }

    private func statusColor(_ status: String) -> Color {
        switch status {
        case "succeeded": .green
        case "failed": .red
        case "cancelled", "interrupted": .orange
        default: .secondary
        }
    }
}

private struct NotebookTaskReplacementSheet: View {
    @ObservedObject var store: WorkbenchStore
    let preview: NotebookTaskReplacementPreview

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            VStack(alignment: .leading, spacing: 5) {
                Text("Replace edited task parameters?")
                    .workbenchFont(.title3, weight: .semibold)
                    .accessibilityIdentifier("notebook.taskReplace.sheet")
                Text("The existing \(preview.intent.surface) task tab has unsaved parameter edits. Review the typed changes before replacing them from this notebook cell.")
                    .foregroundStyle(.secondary)
            }

            Grid(alignment: .leading, horizontalSpacing: 18, verticalSpacing: 8) {
                GridRow {
                    Text("Parameter").workbenchFont(.caption, weight: .semibold)
                    Text("Current edit").workbenchFont(.caption, weight: .semibold)
                    Text("Notebook").workbenchFont(.caption, weight: .semibold)
                }
                Divider().gridCellColumns(3)
                if preview.differences.isEmpty {
                    Text("No resolved value differences; replacing still discards the edited draft state.")
                        .foregroundStyle(.secondary)
                        .gridCellColumns(3)
                } else {
                    ForEach(preview.differences) { difference in
                        GridRow {
                            Text(difference.parameter)
                                .font(.system(size: 12, weight: .semibold, design: .monospaced))
                            diffValue(difference.currentValue)
                            diffValue(difference.notebookValue)
                        }
                        .accessibilityIdentifier("notebook.taskReplace.diff.\(difference.parameter)")
                    }
                }
            }
            .padding(12)
            .background(Color.secondary.opacity(0.06))
            .clipShape(RoundedRectangle(cornerRadius: 7))

            HStack {
                Spacer()
                Button("Cancel") {
                    store.cancelNotebookTaskReplacement()
                }
                .keyboardShortcut(.cancelAction)
                .accessibilityIdentifier("notebook.taskReplace.cancel")
                Button("Replace Parameters") {
                    store.confirmNotebookTaskReplacement()
                }
                .keyboardShortcut(.defaultAction)
                .accessibilityIdentifier("notebook.taskReplace.confirm")
            }
        }
        .padding(22)
        .frame(minWidth: 660, minHeight: 300, alignment: .topLeading)
    }

    private func diffValue(_ value: JSONValue?) -> some View {
        Text(value?.displayText ?? "—")
            .font(.system(size: 12, design: .monospaced))
            .textSelection(.enabled)
    }
}
