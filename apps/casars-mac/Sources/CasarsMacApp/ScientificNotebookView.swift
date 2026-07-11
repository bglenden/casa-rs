import CasarsMacCore
import AppKit
import Foundation
import SwiftUI

struct ScientificNotebookView: View {
    @ObservedObject var store: WorkbenchStore
    @State private var expandedExecutionIDs: Set<String> = []
    @State private var richDocument = PrototypeNotebookRichDocument(markdown: "")

    var body: some View {
        Group {
            if let notebook = store.state.prototypeNotebook {
                notebookWorkspace(notebook)
            } else if store.state.scientificNotebooks?.activeNotebook != nil {
                PersistentScientificNotebookView(store: store)
            } else {
                unavailableNotebook
            }
        }
    }

    private var unavailableNotebook: some View {
        VStack(alignment: .leading, spacing: 14) {
            PanelHeader(
                title: "Scientific Notebook",
                subtitle: "The review prototype runs in a dedicated isolated window."
            )
            Label("Relaunch with --show-prototype notebook", systemImage: "terminal")
                .workbenchFont(.body, design: .monospaced)
                .textSelection(.enabled)
        }
        .padding(28)
        .frame(maxWidth: 560, alignment: .leading)
    }

    private func notebookWorkspace(_ notebook: PrototypeScientificNotebookProjection) -> some View {
        VStack(spacing: 0) {
            notebookToolbar(notebook)
                .padding(.horizontal, 18)
                .padding(.vertical, 11)

            Divider()

            prototypeDisclosure

            Divider()

            ScrollView {
                VStack(alignment: .leading, spacing: 0) {
                    if notebook.hasExternalConflict {
                        externalConflictBanner
                            .padding(.bottom, 20)
                    }

                    notebookDocument(notebook)
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
        .onAppear {
            loadRichDocument(notebook)
        }
        .onChange(of: notebook.notebookID) { _ in
            if let projection = store.state.prototypeNotebook {
                loadRichDocument(projection)
            }
        }
        .onChange(of: notebook.viewMode) { mode in
            if mode == .rich, let projection = store.state.prototypeNotebook {
                loadRichDocument(projection)
            }
        }
    }

    private var prototypeDisclosure: some View {
        HStack(spacing: 8) {
            Image(systemName: "shippingbox.fill")
                .foregroundStyle(.orange)
            Text("Prototype — in memory; no files written")
                .workbenchFont(.caption, weight: .semibold)
            Spacer()
            Text("Fixture controls only · no task, provider, data, or network access")
                .workbenchFont(.caption)
                .foregroundStyle(.primary)
        }
        .padding(.horizontal, 18)
        .padding(.vertical, 8)
        .background(Color.orange.opacity(0.09))
    }

    private func notebookToolbar(_ notebook: PrototypeScientificNotebookProjection) -> some View {
        HStack(spacing: 12) {
            VStack(alignment: .leading, spacing: 2) {
                Text(notebook.title)
                    .workbenchFont(.title3, weight: .semibold)
                Text(notebook.displayPath)
                    .workbenchFont(.caption, design: .monospaced)
                    .foregroundStyle(.primary)
            }

            Spacer()

            Picker("View", selection: Binding(
                get: { notebook.viewMode },
                set: { mode in
                    store.setPrototypeNotebookViewMode(mode)
                }
            )) {
                Label("Rich", systemImage: "doc.richtext")
                    .accessibilityIdentifier("notebook.viewMode.rich")
                    .tag(PrototypeNotebookViewMode.rich)
                Label("Raw", systemImage: "chevron.left.forwardslash.chevron.right")
                    .accessibilityIdentifier("notebook.viewMode.raw")
                    .tag(PrototypeNotebookViewMode.raw)
            }
            .labelsHidden()
            .pickerStyle(.segmented)
            .frame(width: 150)
            .accessibilityIdentifier("notebook.viewMode")

            Button {
                store.savePrototypeNotebookDraft()
            } label: {
                Label("Save", systemImage: "square.and.arrow.down")
            }
            .disabled(!notebook.isDirty || notebook.hasExternalConflict)
            .keyboardShortcut("s", modifiers: [.command])
            .help("Save the in-memory prototype draft")
            .accessibilityIdentifier("notebook.save")

            Text(notebook.isDirty ? "Edited" : "Saved")
                .workbenchFont(.caption, weight: .semibold)
                .foregroundStyle(notebook.isDirty ? .orange : .secondary)
                .accessibilityIdentifier("notebook.dirtyState")
                .accessibilityValue(notebook.isDirty ? "dirty" : "saved")
        }
    }

    private var externalConflictBanner: some View {
        VStack(alignment: .leading, spacing: 9) {
            Label("External edit conflict", systemImage: "exclamationmark.triangle.fill")
                .workbenchFont(.headline)
                .foregroundStyle(.orange)
            Text("The Markdown changed outside CASA-RS while this draft had unsaved edits. Saving is paused until you choose which version to keep.")
                .workbenchFont(.subheadline)
            HStack {
                Button("Keep Local Draft") {
                    store.resolvePrototypeNotebookConflict(keepingDraft: true)
                    if let projection = store.state.prototypeNotebook {
                        loadRichDocument(projection)
                    }
                }
                .accessibilityIdentifier("notebook.conflict.keepDraft")
                Button("Reload External Version") {
                    store.resolvePrototypeNotebookConflict(keepingDraft: false)
                    if let projection = store.state.prototypeNotebook {
                        loadRichDocument(projection)
                    }
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
    private func notebookDocument(_ notebook: PrototypeScientificNotebookProjection) -> some View {
        if notebook.viewMode == .raw {
            VStack(alignment: .leading, spacing: 8) {
                Text("Complete Markdown source")
                    .workbenchFont(.caption, weight: .semibold)
                    .foregroundStyle(.secondary)
                TextEditor(text: markdownBinding)
                    .font(.system(size: 13, design: .monospaced))
                    .scrollContentBackground(.hidden)
                    .padding(10)
                    .frame(minHeight: 680)
                    .background(Color(nsColor: .textBackgroundColor))
                    .overlay(RoundedRectangle(cornerRadius: 6).stroke(Color.secondary.opacity(0.24)))
                    .accessibilityIdentifier("notebook.editor.raw")
            }
        } else {
            VStack(alignment: .leading, spacing: 18) {
                HStack {
                    Label("Rich editing", systemImage: "pencil")
                        .workbenchFont(.caption, weight: .semibold)
                        .foregroundStyle(.primary)
                    Spacer()
                    if notebook.isDirty {
                        Text("Edited")
                            .workbenchFont(.caption, weight: .semibold)
                            .foregroundStyle(.orange)
                    }
                }

                ForEach(richDocument.elements) { element in
                    if let receiptID = element.taskID,
                       let receipt = notebook.task(receiptID: receiptID) {
                        inlineTaskBlock(receipt)
                    } else if element.taskID != nil {
                        Text(element.source)
                            .font(.system(size: 12, design: .monospaced))
                            .textSelection(.enabled)
                            .padding(10)
                            .frame(maxWidth: .infinity, alignment: .leading)
                            .background(Color.secondary.opacity(0.065))
                            .clipShape(RoundedRectangle(cornerRadius: 5))
                    } else {
                        RichMarkdownBlockEditor(
                            source: richElementBinding(element.id),
                            headingLevel: element.headingLevel,
                            isInsertionSurface: element.isInsertionSurface,
                            accessibilityID: "notebook.richElement.\(element.id)"
                        )
                    }
                }
            }
        }
    }

    private var markdownBinding: Binding<String> {
        Binding(
            get: { store.state.prototypeNotebook?.draftMarkdown ?? "" },
            set: { store.setPrototypeNotebookDraft($0) }
        )
    }

    private func richElementBinding(_ elementID: String) -> Binding<String> {
        Binding(
            get: {
                richDocument.elements
                    .first(where: { $0.id == elementID })?
                    .editableSource ?? ""
            },
            set: { value in
                var updated = richDocument
                guard updated.replaceEditableSource(elementID: elementID, with: value) else {
                    return
                }
                richDocument = updated
                store.setPrototypeNotebookDraft(updated.markdown)
            }
        )
    }

    private func loadRichDocument(_ notebook: PrototypeScientificNotebookProjection) {
        richDocument = PrototypeNotebookRichDocument(markdown: notebook.draftMarkdown)
    }

    private func inlineTaskBlock(_ receipt: PrototypeNotebookTaskProjection) -> some View {
        VStack(alignment: .leading, spacing: 5) {
            Button {
                store.openPrototypeNotebookTask(receiptID: receipt.id)
            } label: {
                parameterBlockLabel(receipt)
            }
            .buttonStyle(.plain)
            .help("Open \(receipt.taskID) in a task tab with these parameters")
            .accessibilityIdentifier("notebook.parameters.open.\(receipt.id)")

            executionStrip(receipt)
        }
        .accessibilityElement(children: .contain)
    }

    private func parameterBlockLabel(_ receipt: PrototypeNotebookTaskProjection) -> some View {
        VStack(alignment: .leading, spacing: 6) {
            HStack(spacing: 8) {
                Text("# \(receipt.title)")
                    .font(.system(size: 12, weight: .semibold, design: .monospaced))
                    .foregroundStyle(.secondary)
                Spacer()
                Image(systemName: "arrow.up.forward.app")
                    .workbenchFont(.caption2)
                    .foregroundStyle(.tertiary)
            }

            Text("[parameters]")
                .font(.system(size: 12, design: .monospaced))
                .foregroundStyle(.secondary)

            ForEach(receipt.parameterRows) { parameter in
                Text("\(parameter.parameterID) = \(tomlDisplayValue(parameter.value))")
                    .font(.system(size: 12, design: .monospaced))
                    .lineLimit(1)
                    .truncationMode(.middle)
                    .accessibilityLabel("\(parameter.label), \(parameter.value)")
            }
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 10)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(Color.secondary.opacity(0.065))
        .clipShape(RoundedRectangle(cornerRadius: 5))
        .contentShape(Rectangle())
    }

    private func tomlDisplayValue(_ value: String) -> String {
        if Double(value) != nil || value == "true" || value == "false" {
            return value
        }
        return "\"\(value)\""
    }

    private func executionStrip(_ receipt: PrototypeNotebookTaskProjection) -> some View {
        let revision = receipt.latestRevision
        let status = revision?.status
        let expanded = expandedExecutionIDs.contains(receipt.id)

        return VStack(alignment: .leading, spacing: 6) {
            Button {
                toggleExecution(receipt.id)
            } label: {
                HStack(spacing: 5) {
                    Circle()
                        .fill(status?.notebookColor ?? .secondary)
                        .frame(width: 6, height: 6)
                    Text(status?.notebookLabel ?? "Not run")
                        .workbenchFont(.caption2, weight: .semibold)
                        .foregroundStyle(.secondary)
                    Text("r\(revision?.sequence ?? 0)")
                        .workbenchFont(.caption2, design: .monospaced)
                        .foregroundStyle(.tertiary)
                    Image(systemName: expanded ? "chevron.up" : "chevron.down")
                        .workbenchFont(.caption2)
                        .foregroundStyle(.tertiary)
                }
            }
            .buttonStyle(.plain)
            .highPriorityGesture(
                TapGesture(count: 2).onEnded {
                    store.openPrototypeNotebookTask(receiptID: receipt.id)
                }
            )
            .help("Click for the result; double-click to open the task tab")
            .accessibilityIdentifier("notebook.executionStatus.\(receipt.id)")
            .accessibilityValue("\(status?.notebookLabel ?? "Not run"), revision \(revision?.sequence ?? 0), \(expanded ? "expanded" : "collapsed")")

            if expanded {
                expandedExecution(receipt)
                    .padding(.leading, 11)
            }
        }
        .accessibilityElement(children: .contain)
    }

    private func expandedExecution(_ receipt: PrototypeNotebookTaskProjection) -> some View {
        VStack(alignment: .leading, spacing: 9) {
            HStack(spacing: 8) {
                Button("Restart Fixture") {
                    store.restartPrototypeNotebookTask(receiptID: receipt.id)
                }
                .disabled(receipt.revisions.contains { $0.status == .running })
                .accessibilityIdentifier("notebook.execution.restart.\(receipt.id)")

                if receipt.latestRevision?.status == .running {
                    Button("Complete") {
                        store.completePrototypeNotebookTaskRun(receiptID: receipt.id)
                    }
                    .accessibilityIdentifier("notebook.execution.complete.\(receipt.id)")
                    Button("Cancel", role: .destructive) {
                        store.cancelPrototypeNotebookTaskRun(receiptID: receipt.id)
                    }
                    .accessibilityIdentifier("notebook.execution.cancel.\(receipt.id)")
                }

                Spacer()

                Text("\(receipt.revisions.count) revision\(receipt.revisions.count == 1 ? "" : "s")")
                    .workbenchFont(.caption)
                    .foregroundStyle(.secondary)
                    .accessibilityIdentifier("notebook.execution.revisionCount.\(receipt.id)")
            }

            if let revision = receipt.latestRevision {
                Text(revision.timestamp)
                    .workbenchFont(.caption, design: .monospaced)
                    .foregroundStyle(.secondary)

                if !revision.products.isEmpty {
                    Label(revision.products.joined(separator: " · "), systemImage: "shippingbox")
                        .workbenchFont(.caption, design: .monospaced)
                        .textSelection(.enabled)
                }
            }
        }
    }

    private func toggleExecution(_ receiptID: String) {
        if expandedExecutionIDs.contains(receiptID) {
            expandedExecutionIDs.remove(receiptID)
        } else {
            expandedExecutionIDs.insert(receiptID)
            store.selectPrototypeNotebookReceipt(receiptID)
        }
    }
}

struct PrototypeNotebookTaskView: View {
    @ObservedObject var store: WorkbenchStore
    let tab: WorkbenchTab
    @State private var editedValues: [String: String] = [:]

    var body: some View {
        Group {
            if let task = taskProjection {
                taskWorkspace(task)
                    .onAppear { loadFixtureValues(task) }
                    .onChange(of: task.id) { _ in loadFixtureValues(task) }
            } else {
                VStack(alignment: .leading, spacing: 12) {
                    PanelHeader(title: "Prototype task unavailable", subtitle: "Return to the notebook and reopen its parameter block.")
                }
                .padding(28)
            }
        }
    }

    private var taskProjection: PrototypeNotebookTaskProjection? {
        guard let receiptID = tab.prototypeReceiptID,
              let notebook = store.state.prototypeNotebook
        else { return nil }
        return notebook.documents.lazy.compactMap { document in
            document.tasks.first { $0.id == receiptID }
        }.first
    }

    private func taskWorkspace(_ task: PrototypeNotebookTaskProjection) -> some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 18) {
                HStack(alignment: .top) {
                    PanelHeader(
                        title: task.title,
                        subtitle: "\(task.taskID) · parameters loaded from the notebook block"
                    )
                    .accessibilityIdentifier("prototypeTask.identity.\(task.id)")
                    Spacer()
                    Button {
                        store.restartPrototypeNotebookTask(receiptID: task.id)
                    } label: {
                        Label("Restart Fixture", systemImage: "arrow.clockwise")
                    }
                    .buttonStyle(.borderedProminent)
                    .disabled(task.revisions.contains { $0.status == .running })
                    .accessibilityIdentifier("prototypeTask.restart")
                }

                HStack(spacing: 8) {
                    Image(systemName: "shippingbox.fill")
                        .foregroundStyle(.orange)
                    Text("Prototype — editable fixture form; Run never reaches a provider or the filesystem")
                        .workbenchFont(.caption, weight: .semibold)
                }
                .padding(10)
                .frame(maxWidth: .infinity, alignment: .leading)
                .background(Color.orange.opacity(0.09))
                .clipShape(RoundedRectangle(cornerRadius: 7))

                VStack(alignment: .leading, spacing: 12) {
                    Text("Parameters")
                        .workbenchFont(.headline)
                    ForEach(task.parameterRows) { parameter in
                        HStack(alignment: .firstTextBaseline, spacing: 16) {
                            VStack(alignment: .leading, spacing: 2) {
                                Text(parameter.label)
                                    .workbenchFont(.subheadline, weight: .semibold)
                                Text(parameter.parameterID)
                                    .workbenchFont(.caption, design: .monospaced)
                                    .foregroundStyle(.secondary)
                            }
                            .frame(width: 180, alignment: .leading)

                            TextField(parameter.label, text: Binding(
                                get: { editedValues[parameter.parameterID] ?? parameter.value },
                                set: { editedValues[parameter.parameterID] = $0 }
                            ))
                            .textFieldStyle(.roundedBorder)
                            .font(.system(size: 13, design: .monospaced))
                            .accessibilityIdentifier("prototypeTask.parameter.\(parameter.parameterID)")
                        }
                    }
                }
                .notebookCard()

                taskExecutionDetails(task)
            }
            .padding(28)
            .frame(maxWidth: 900, alignment: .leading)
            .frame(maxWidth: .infinity)
        }
    }

    private func taskExecutionDetails(_ task: PrototypeNotebookTaskProjection) -> some View {
        VStack(alignment: .leading, spacing: 13) {
            HStack {
                Text("Execution revisions")
                    .workbenchFont(.headline)
                Spacer()
                if task.latestRevision?.status == .running {
                    Button("Complete") {
                        store.completePrototypeNotebookTaskRun(receiptID: task.id)
                    }
                    .accessibilityIdentifier("prototypeTask.complete")
                    Button("Cancel", role: .destructive) {
                        store.cancelPrototypeNotebookTaskRun(receiptID: task.id)
                    }
                    .accessibilityIdentifier("prototypeTask.cancel")
                }
            }

            ForEach(task.revisions.sorted { $0.sequence > $1.sequence }) { revision in
                VStack(alignment: .leading, spacing: 7) {
                    HStack {
                        Image(systemName: revision.status.notebookIcon)
                            .foregroundStyle(revision.status.notebookColor)
                        Text("Revision \(revision.sequence) · \(revision.status.notebookLabel)")
                            .workbenchFont(.subheadline, weight: .semibold)
                        Spacer()
                        Text(revision.timestamp)
                            .workbenchFont(.caption, design: .monospaced)
                            .foregroundStyle(.secondary)
                    }
                    Text(revision.summary)
                        .workbenchFont(.subheadline)
                    detailSection("Products", values: revision.products, monospaced: true)
                    detailSection("Log", values: revision.logLines, monospaced: true)
                    detailSection("Diagnostics", values: revision.diagnostics, monospaced: false)
                }
                .padding(12)
                .background(revision.status.notebookColor.opacity(0.055))
                .clipShape(RoundedRectangle(cornerRadius: 7))
                .accessibilityIdentifier("prototypeTask.revision.\(revision.sequence).\(revision.status.rawValue)")
            }
        }
        .notebookCard()
    }

    @ViewBuilder
    private func detailSection(_ title: String, values: [String], monospaced: Bool) -> some View {
        if !values.isEmpty {
            VStack(alignment: .leading, spacing: 3) {
                Text(title)
                    .workbenchFont(.caption, weight: .semibold)
                    .foregroundStyle(.secondary)
                ForEach(values, id: \.self) { value in
                    Text(value)
                        .font(.system(size: 11, design: monospaced ? .monospaced : .default))
                        .textSelection(.enabled)
                }
            }
        }
    }

    private func loadFixtureValues(_ task: PrototypeNotebookTaskProjection) {
        if editedValues.isEmpty {
            editedValues = Dictionary(uniqueKeysWithValues: task.parameterRows.map { ($0.parameterID, $0.value) })
        }
    }
}

struct RichMarkdownBlockEditor: View {
    @Binding var source: String
    let headingLevel: Int?
    let isInsertionSurface: Bool
    let accessibilityID: String

    var body: some View {
        if let headingLevel {
            TextField("Heading", text: Binding(
                get: { source },
                set: { source = $0 }
            ))
            .textFieldStyle(.plain)
            .font(headingFont(level: headingLevel))
            .fontWeight(.semibold)
            .accessibilityIdentifier(accessibilityID)
        } else {
            ZStack(alignment: .topLeading) {
                if source.isEmpty {
                    Text(isInsertionSurface ? "Add notes here…" : "Continue writing notes…")
                        .foregroundStyle(.tertiary)
                        .padding(.horizontal, 5)
                        .padding(.vertical, 8)
                }
                TextEditor(text: $source)
                    .font(.system(size: 15))
                    .scrollContentBackground(.hidden)
                    .frame(minHeight: editorHeight)
                    .fixedSize(horizontal: false, vertical: true)
                    .accessibilityIdentifier(accessibilityID)
            }
        }
    }

    private func headingFont(level: Int) -> Font {
        switch level {
        case 1: .title
        case 2: .title2
        case 3: .title3
        default: .headline
        }
    }

    private var editorHeight: CGFloat {
        max(46, CGFloat(source.components(separatedBy: .newlines).count) * 23 + 18)
    }
}

private extension View {
    func notebookCard() -> some View {
        padding(13)
            .frame(maxWidth: .infinity, alignment: .leading)
            .background(.regularMaterial)
            .overlay(RoundedRectangle(cornerRadius: 9).stroke(Color.secondary.opacity(0.22)))
            .clipShape(RoundedRectangle(cornerRadius: 9))
    }
}

private extension PrototypeNotebookReceiptStatus {
    var notebookLabel: String {
        switch self {
        case .running: "Running"
        case .succeeded: "Succeeded"
        case .failed: "Failed"
        case .cancelled: "Cancelled"
        }
    }

    var notebookIcon: String {
        switch self {
        case .running: "progress.indicator"
        case .succeeded: "checkmark.circle.fill"
        case .failed: "xmark.octagon.fill"
        case .cancelled: "stop.circle.fill"
        }
    }

    var notebookColor: Color {
        switch self {
        case .running: .yellow
        case .succeeded: .green
        case .failed: .red
        case .cancelled: .orange
        }
    }
}
