import CasarsMacCore
import SwiftUI

struct PersistentScientificNotebookView: View {
    @ObservedObject var store: WorkbenchStore
    @State private var richDocument = PrototypeNotebookRichDocument(markdown: "")
    @State private var expandedCellIDs: Set<String> = []
    @State private var expandedPythonHistory: Set<String> = []
    @State private var lightboxRevision: NotebookVisualizationRevision?

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
        .sheet(item: $lightboxRevision) { revision in
            if let root = store.state.scientificNotebooks?.projectRoot,
               let image = NSImage(contentsOfFile: URL(fileURLWithPath: root)
                .appendingPathComponent(revision.assetPath).path)
            {
                ScrollView([.horizontal, .vertical]) {
                    Image(nsImage: image).resizable().scaledToFit().padding(24)
                }
                .frame(minWidth: 720, minHeight: 520)
                .accessibilityIdentifier("notebook.visualization.lightbox")
            }
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
            if document.cells.contains(where: { $0.kind == "python" }) {
                Text("User Python · normal user authority")
                    .workbenchFont(.caption, weight: .semibold)
                    .foregroundStyle(.secondary)
                    .accessibilityIdentifier("notebook.python.authority")
                Button("Run All") { store.runAllScientificPythonCells() }
                    .disabled(store.pythonNotebookRuntime.status != .ready)
                    .accessibilityIdentifier("notebook.python.runAll")
                Button("Stop") { store.interruptScientificPythonKernel() }
                    .disabled(store.pythonNotebookRuntime.status != .running)
                    .accessibilityIdentifier("notebook.python.stop")
                Button("Restart") { store.restartScientificPythonKernel() }
                    .disabled(![.restartRequired, .interrupting].contains(store.pythonNotebookRuntime.status))
                    .accessibilityIdentifier("notebook.python.restart")
                if store.projectPythonEnvironmentStatus == .unavailable {
                    Button("Create Python Environment") {
                        store.createOrRepairProjectPythonEnvironment()
                    }
                    .accessibilityIdentifier("notebook.python.createEnvironment")
                } else {
                    Button("Install Plotting") { store.installProjectPythonPlottingPackages() }
                        .accessibilityIdentifier("notebook.python.installPlotting")
                }
            }
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
        if let visualization = document.visualizations.first(where: { $0.cellId == cellID }) {
            visualizationCard(visualization, document: document)
        } else if let cell = document.cells.first(where: { $0.id == cellID }), cell.kind == "python" {
            pythonCell(cell, receipts: receipts, document: document)
        } else if let intent = document.cells.first(where: { $0.id == cellID })?.taskIntent {
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

    private func visualizationCard(
        _ visualization: NotebookVisualizationSnapshot,
        document: NotebookDocumentState
    ) -> some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack {
                Text(visualization.title).workbenchFont(.headline)
                Spacer()
                Button("Open in Explorer") {
                    store.openNotebookVisualization(visualization.id)
                }
                .accessibilityIdentifier("notebook.visualization.openExplorer.\(visualization.id)")
            }
            if let latest = visualization.revisions.last {
                visualizationPreview(latest, document: document, height: 300)
                Text("Revision \(latest.revision) · \(latest.reopen.surface)")
                    .workbenchFont(.caption, design: .monospaced)
                    .foregroundStyle(.secondary)
                if visualization.revisions.count > 1 {
                    DisclosureGroup("Previous revisions (\(visualization.revisions.count - 1))") {
                        VStack(alignment: .leading, spacing: 8) {
                            ForEach(Array(visualization.revisions.dropLast()).reversed()) { revision in
                                visualizationPreview(revision, document: document, height: 130)
                            }
                        }
                        .padding(.top, 6)
                    }
                    .accessibilityIdentifier("notebook.visualization.previousRevisions.\(visualization.id)")
                }
            }
        }
        .padding(10)
        .background(Color.secondary.opacity(0.045))
        .clipShape(RoundedRectangle(cornerRadius: 7))
        .accessibilityIdentifier("notebook.visualization.\(visualization.id)")
    }

    private func visualizationPreview(
        _ revision: NotebookVisualizationRevision,
        document: NotebookDocumentState,
        height: CGFloat
    ) -> some View {
        let path = documentPath(document, revision.assetPath)
        return Button {
            lightboxRevision = revision
        } label: {
            if let image = NSImage(contentsOfFile: path) {
                Image(nsImage: image)
                    .resizable()
                    .scaledToFit()
                    .frame(maxWidth: .infinity, maxHeight: height)
            } else {
                Text(revision.assetPath).workbenchFont(.caption, design: .monospaced)
            }
        }
        .buttonStyle(.plain)
        .accessibilityIdentifier("notebook.visualization.preview.\(revision.revision)")
    }

    private func pythonCell(
        _ cell: NotebookCellState,
        receipts: [NotebookExecutionReceipt],
        document: NotebookDocumentState
    ) -> some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack {
                Text("Python")
                    .workbenchFont(.caption, weight: .semibold)
                    .foregroundStyle(.secondary)
                Text(store.pythonNotebookRuntime.status.rawValue.replacingOccurrences(of: "_", with: " "))
                    .workbenchFont(.caption2, weight: .semibold)
                    .foregroundStyle(.secondary)
                    .accessibilityIdentifier("notebook.python.kernelState")
                Spacer()
                Button(receipts.isEmpty ? "Run" : "Regenerate") {
                    store.runScientificPythonCell(cell.id)
                }
                .disabled(store.pythonNotebookRuntime.status != .ready)
                .accessibilityIdentifier("notebook.python.run.\(cell.id)")
            }
            TextEditor(text: Binding(
                get: { pythonSource(cell.body) },
                set: { store.setScientificPythonSource(cellID: cell.id, source: $0) }
            ))
            .font(.system(size: 12, design: .monospaced))
            .scrollContentBackground(.hidden)
            .frame(minHeight: 110)
            .padding(8)
            .background(Color.secondary.opacity(0.055))
            .clipShape(RoundedRectangle(cornerRadius: 5))
            .accessibilityIdentifier("notebook.python.editor.\(cell.id)")

            if let latest = receipts.first {
                pythonRevision(latest, document: document, prominent: true)
                if receipts.count > 1 {
                    DisclosureGroup(
                        isExpanded: Binding(
                            get: { expandedPythonHistory.contains(cell.id) },
                            set: { expanded in
                                if expanded { expandedPythonHistory.insert(cell.id) }
                                else { expandedPythonHistory.remove(cell.id) }
                            }
                        )
                    ) {
                        VStack(alignment: .leading, spacing: 7) {
                            ForEach(Array(receipts.dropFirst())) { revision in
                                pythonRevision(revision, document: document, prominent: false)
                            }
                        }
                        .padding(.top, 6)
                    } label: {
                        Text("Previous revisions (\(receipts.count - 1))")
                            .workbenchFont(.caption, weight: .semibold)
                    }
                    .accessibilityIdentifier("notebook.python.previousRevisions.\(cell.id)")
                }
            }
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 10)
        .overlay(RoundedRectangle(cornerRadius: 6).stroke(Color.secondary.opacity(0.2)))
        .accessibilityIdentifier("notebook.python.cell.\(cell.id)")
    }

    private func pythonRevision(
        _ receipt: NotebookExecutionReceipt,
        document: NotebookDocumentState,
        prominent: Bool
    ) -> some View {
        VStack(alignment: .leading, spacing: 5) {
            HStack(spacing: 6) {
                Circle().fill(statusColor(receipt.status)).frame(width: 6, height: 6)
                Text("\(receipt.status.capitalized) · r\(receipt.revision)")
                    .workbenchFont(.caption2, weight: .semibold)
                Spacer()
                if let environment = receipt.executionInput?.details.environment {
                    Text("\(environment.implementation) \(environment.version)")
                        .workbenchFont(.caption2, design: .monospaced)
                        .foregroundStyle(.secondary)
                }
            }
            ForEach(receipt.orderedOutputs ?? []) { output in
                Text(output.text)
                    .font(.system(size: 11, design: .monospaced))
                    .foregroundStyle(output.channel == "stderr" ? Color.red : Color.primary)
                    .textSelection(.enabled)
            }
            ForEach(receipt.diagnostics, id: \.self) { diagnostic in
                Text(diagnostic)
                    .workbenchFont(.caption, design: .monospaced)
                    .foregroundStyle(.red)
                    .textSelection(.enabled)
            }
            let figures = receipt.artifacts.filter { $0.mediaType == "image/png" }
            ForEach(figures, id: \.path) { artifact in
                let path = URL(fileURLWithPath: documentPath(document, artifact.path)).path
                if let image = NSImage(contentsOfFile: path) {
                    Image(nsImage: image)
                        .resizable()
                        .scaledToFit()
                        .frame(maxHeight: prominent ? 320 : 140)
                        .accessibilityIdentifier("notebook.python.figure.\(receipt.id)")
                }
            }
            if !receipt.artifacts.isEmpty {
                Text(receipt.artifacts.map { "\($0.role): \($0.path)" }.joined(separator: " · "))
                    .workbenchFont(.caption2, design: .monospaced)
                    .foregroundStyle(.secondary)
                    .textSelection(.enabled)
            }
        }
        .padding(8)
        .background(Color.secondary.opacity(prominent ? 0.05 : 0.025))
        .clipShape(RoundedRectangle(cornerRadius: 5))
        .accessibilityIdentifier(
            prominent
                ? "notebook.python.latestRevision.\(receipt.cellId)"
                : "notebook.python.revision.\(receipt.id)"
        )
        .accessibilityValue(
            (["\(receipt.status) revision \(receipt.revision)"]
                + (receipt.orderedOutputs ?? []).map { "\($0.channel): \($0.text)" })
                .joined(separator: "\n")
        )
    }

    private func pythonSource(_ body: String) -> String {
        var lines = body.split(separator: "\n", omittingEmptySubsequences: false)
        while lines.last?.isEmpty == true { lines.removeLast() }
        guard let first = lines.first,
              first.trimmingCharacters(in: .whitespaces).hasPrefix("```python"),
              let last = lines.last,
              last.trimmingCharacters(in: .whitespaces).hasPrefix("```")
        else { return body }
        return lines.dropFirst().dropLast().joined(separator: "\n")
    }

    private func documentPath(_ document: NotebookDocumentState, _ relative: String) -> String {
        guard let root = store.state.scientificNotebooks?.projectRoot else { return relative }
        return URL(fileURLWithPath: root).appendingPathComponent(relative).path
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
