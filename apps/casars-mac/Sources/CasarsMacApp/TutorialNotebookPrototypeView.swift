import CasarsMacCore
import SwiftUI

struct TutorialNotebookPrototypeView: View {
    @ObservedObject var store: WorkbenchStore
    @State private var expandedFailureDetails = false
    @State private var learnerRichDocument = PrototypeNotebookRichDocument(markdown: "")

    private var tutorial: TutorialNotebookPrototypeProjection? {
        store.state.prototypeTutorial
    }

    private var learnerNotebook: PrototypeScientificNotebookProjection? {
        tutorial?.learnerNotebook
    }

    var body: some View {
        VStack(spacing: 0) {
            toolbar
            Divider()
            prototypeDisclosure
            Divider()
            document
        }
        .sheet(isPresented: approvalPresented) {
            approvalSheet
        }
        .onAppear {
            loadRichDocument()
        }
        .onChange(of: learnerNotebook?.notebookID) { _ in
            loadRichDocument()
        }
        .onChange(of: learnerNotebook?.viewMode) { mode in
            if mode == .rich {
                loadRichDocument()
            }
        }
    }

    private var toolbar: some View {
        HStack(spacing: 12) {
            VStack(alignment: .leading, spacing: 2) {
                Text(tutorial?.title ?? "Tutorial notebook")
                    .workbenchFont(.title3, weight: .semibold)
                Text(learnerNotebook?.displayPath ?? "notebooks/TW Hya First Look.md")
                    .workbenchFont(.caption, design: .monospaced)
                    .foregroundStyle(.secondary)
            }

            Spacer()

            HStack(spacing: 6) {
                Circle()
                    .fill(Color.accentColor)
                    .frame(width: 7, height: 7)
                Text(progressLabel)
                    .workbenchFont(.caption, weight: .semibold)
            }
            .accessibilityElement(children: .combine)
            .accessibilityIdentifier("tutorialPrototype.progressSummary")
            .accessibilityValue(progressLabel)

            if let learnerNotebook {
                Picker("View", selection: Binding(
                    get: { learnerNotebook.viewMode },
                    set: { store.setTutorialPrototypeViewMode($0) }
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
            }

            Button {
                store.saveTutorialPrototypeDraft()
            } label: {
                Label("Save", systemImage: "square.and.arrow.down")
            }
            .disabled(learnerNotebook?.isDirty != true)
            .keyboardShortcut("s", modifiers: [.command])
            .accessibilityIdentifier("notebook.save")

            Text(learnerNotebook?.isDirty == true ? "Edited" : "Saved")
                .workbenchFont(.caption, weight: .semibold)
                .foregroundStyle(learnerNotebook?.isDirty == true ? .orange : .secondary)
                .accessibilityIdentifier("notebook.dirtyState")
                .accessibilityValue(learnerNotebook?.isDirty == true ? "dirty" : "saved")
        }
        .padding(.horizontal, 18)
        .padding(.vertical, 11)
    }

    private var prototypeDisclosure: some View {
        HStack(spacing: 9) {
            Image(systemName: "testtube.2")
                .foregroundStyle(.blue)
                .accessibilityHidden(true)
            Text("Prototype — deterministic fixtures only. No network, file, archive, task, or durable project operation is used.")
                .workbenchFont(.caption, weight: .semibold)
            Spacer()
            Text("Boundary calls: \(store.prototypeProductionBoundaryInvocationCount)")
                .workbenchFont(.caption, weight: .semibold, design: .monospaced)
                .accessibilityIdentifier("tutorialPrototype.boundaryAudit")
                .accessibilityValue("\(store.prototypeProductionBoundaryInvocationCount)")
        }
        .padding(.horizontal, 18)
        .padding(.vertical, 8)
        .background(Color.blue.opacity(0.07))
    }

    @ViewBuilder
    private var document: some View {
        if learnerNotebook?.viewMode == .raw {
            rawDocument
        } else {
            richDocument
        }
    }

    private var rawDocument: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 8) {
                Text("Complete learner Markdown")
                    .workbenchFont(.caption, weight: .semibold)
                    .foregroundStyle(.secondary)
                TextEditor(text: Binding(
                    get: { learnerNotebook?.draftMarkdown ?? "" },
                    set: { store.setTutorialPrototypeDraft($0) }
                ))
                .font(.system(size: 13, design: .monospaced))
                .scrollContentBackground(.hidden)
                .padding(10)
                .frame(minHeight: 680)
                .background(Color(nsColor: .textBackgroundColor))
                .overlay(RoundedRectangle(cornerRadius: 6).stroke(Color.secondary.opacity(0.24)))
                .accessibilityIdentifier("notebook.editor.raw")
            }
            .padding(.horizontal, 44)
            .padding(.top, 28)
            .padding(.bottom, 80)
            .frame(maxWidth: 920, alignment: .leading)
            .frame(maxWidth: .infinity)
        }
        .background(Color(nsColor: .textBackgroundColor))
        .accessibilityIdentifier("notebook.document.scroll")
    }

    private var richDocument: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 24) {
                compactSectionProgress

                if acquisitionAnchorElementID == nil {
                    acquisitionCard
                }

                ForEach(learnerRichDocument.elements) { element in
                    richElement(element)

                    if element.id == acquisitionAnchorElementID {
                        acquisitionCard
                    }
                }

                Text("End of learner notebook")
                    .workbenchFont(.caption)
                    .foregroundStyle(.tertiary)
                    .padding(.bottom, 70)
            }
            .padding(.horizontal, 44)
            .padding(.top, 28)
            .frame(maxWidth: 920, alignment: .leading)
            .frame(maxWidth: .infinity)
        }
        .background(Color(nsColor: .textBackgroundColor))
        .accessibilityIdentifier("notebook.document.scroll")
    }

    /// The acquisition control is a fixture decoration rather than Markdown
    /// content. Place it after the first section's prose when possible while
    /// keeping every Markdown element sourced from the learner draft.
    private var acquisitionAnchorElementID: String? {
        let elements = learnerRichDocument.elements
        guard let headingIndex = elements.firstIndex(where: { $0.headingLevel == 2 }) else {
            return nil
        }
        let followingIndex = elements.index(after: headingIndex)
        if followingIndex < elements.endIndex,
           elements[followingIndex].headingLevel == nil,
           elements[followingIndex].taskID == nil {
            return elements[followingIndex].id
        }
        return elements[headingIndex].id
    }

    @ViewBuilder
    private func richElement(_ element: PrototypeNotebookRichElement) -> some View {
        if let taskID = element.taskID {
            if taskID == tutorial?.fixtureTask.id {
                taskParameterCard
            } else {
                Text(element.source)
                    .font(.system(size: 12, design: .monospaced))
                    .textSelection(.enabled)
                    .padding(10)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .background(Color.secondary.opacity(0.065))
                    .clipShape(RoundedRectangle(cornerRadius: 5))
            }
        } else {
            RichMarkdownBlockEditor(
                source: richElementBinding(element.id),
                headingLevel: element.headingLevel,
                isInsertionSurface: element.isInsertionSurface,
                accessibilityID: "notebook.richElement.\(element.id)"
            )
        }
    }

    private var compactSectionProgress: some View {
        HStack(spacing: 14) {
            ForEach(tutorial?.sections ?? []) { section in
                Button {
                    store.selectTutorialPrototypeSection(section.id)
                } label: {
                    HStack(spacing: 5) {
                        Circle()
                            .fill(section.status.color)
                            .frame(width: 7, height: 7)
                        Text(section.title)
                            .workbenchFont(.caption, weight: section.id == tutorial?.selectedSectionID ? .semibold : .regular)
                            .lineLimit(1)
                    }
                }
                .buttonStyle(.plain)
                .disabled(section.status == .blocked)
                .accessibilityIdentifier("tutorialPrototype.section.select.\(section.id)")
                .accessibilityValue(section.status.rawValue)
            }
            Spacer(minLength: 0)
        }
        .padding(.vertical, 2)
    }

    private func richElementBinding(_ elementID: String) -> Binding<String> {
        Binding(
            get: {
                learnerRichDocument.elements
                    .first(where: { $0.id == elementID })?
                    .editableSource ?? ""
            },
            set: { value in
                var updated = learnerRichDocument
                guard updated.replaceEditableSource(elementID: elementID, with: value) else {
                    return
                }
                learnerRichDocument = updated
                store.setTutorialPrototypeDraft(updated.markdown)
            }
        )
    }

    private func loadRichDocument() {
        learnerRichDocument = PrototypeNotebookRichDocument(
            markdown: learnerNotebook?.draftMarkdown ?? ""
        )
    }

    @ViewBuilder
    private var acquisitionCard: some View {
        if let dataset = tutorial?.dataset {
            VStack(alignment: .leading, spacing: 9) {
                HStack(spacing: 9) {
                    Image(systemName: dataset.phase.icon)
                        .foregroundStyle(dataset.phase.color)
                        .accessibilityHidden(true)
                    VStack(alignment: .leading, spacing: 2) {
                        Text(dataset.name)
                            .workbenchFont(.subheadline, weight: .semibold)
                        Text(dataset.destination)
                            .workbenchFont(.caption, design: .monospaced)
                            .foregroundStyle(.primary)
                    }
                    Spacer()
                    Text(dataset.phase.label)
                        .workbenchFont(.caption, weight: .semibold)
                        .foregroundStyle(dataset.phase.color)
                        .accessibilityIdentifier("tutorialPrototype.dataset.status.\(dataset.id)")
                        .accessibilityValue(dataset.phase.rawValue)
                }

                if dataset.phase.isRunning {
                    let progress = dataset.currentAttempt?.progress ?? 0
                    let progressPercent = Int(progress * 100)
                    HStack(spacing: 8) {
                        ProgressView(value: progress)
                            .progressViewStyle(.linear)
                        Text("\(progressPercent)%")
                            .workbenchFont(.caption2, design: .monospaced)
                            .foregroundStyle(.secondary)
                            .accessibilityIdentifier("tutorialPrototype.dataset.progress.\(dataset.id)")
                    }
                }

                if let message = dataset.message,
                   dataset.phase != .missing,
                   dataset.phase != .ready {
                    Text(message)
                        .workbenchFont(.caption)
                        .foregroundStyle(.primary)
                        .lineLimit(2)
                }

                acquisitionActions(dataset)

                if let attempt = dataset.currentAttempt {
                    let effectiveResumeOffset = dataset.phase == .cancelled || dataset.phase == .offline
                        ? attempt.downloadedBytes
                        : attempt.resumeOffsetBytes
                    HStack(spacing: 14) {
                        Text("Attempt \(attempt.generation)")
                            .accessibilityIdentifier("tutorialPrototype.dataset.attempt.\(dataset.id)")
                            .accessibilityValue("\(attempt.generation)")
                        Text("Resume offset \(formatBytes(effectiveResumeOffset))")
                            .accessibilityIdentifier("tutorialPrototype.dataset.resumeOffset.\(dataset.id)")
                            .accessibilityValue("\(effectiveResumeOffset)")
                    }
                    .workbenchFont(.caption2, design: .monospaced)
                    .foregroundStyle(.tertiary)
                }

                if dataset.phase.isFailure {
                    Button {
                        expandedFailureDetails.toggle()
                    } label: {
                        HStack(spacing: 5) {
                            Image(systemName: expandedFailureDetails ? "chevron.down" : "chevron.right")
                                .imageScale(.small)
                            Text("Details")
                            Spacer()
                        }
                        .contentShape(Rectangle())
                    }
                    .buttonStyle(.plain)
                    .workbenchFont(.caption, weight: .semibold)
                    .accessibilityIdentifier("tutorialPrototype.failure.details.\(dataset.id)")
                    .accessibilityValue(expandedFailureDetails ? "expanded" : "collapsed")

                    if expandedFailureDetails {
                        VStack(alignment: .leading, spacing: 5) {
                            Text(dataset.message ?? "The fixture acquisition did not complete.")
                            Text("No incomplete or unverified dataset is visible to the project.")
                        }
                        .workbenchFont(.caption, design: .monospaced)
                        .foregroundStyle(.secondary)
                        .padding(.top, 5)
                    }
                }

                if dataset.phase == .ready {
                    Text("Optional checks: MeasurementSet structure · TW Hya target field")
                        .workbenchFont(.caption)
                        .foregroundStyle(.secondary)
                }
            }
            .padding(12)
            .background(Color.secondary.opacity(0.035))
            .overlay(RoundedRectangle(cornerRadius: 7).stroke(Color.secondary.opacity(0.17)))
            .clipShape(RoundedRectangle(cornerRadius: 7))
            .accessibilityElement(children: .contain)
            .accessibilityIdentifier("tutorialPrototype.dataset.\(dataset.id)")
        }
    }

    @ViewBuilder
    private func acquisitionActions(_ dataset: TutorialNotebookDatasetProjection) -> some View {
        HStack(spacing: 8) {
            switch dataset.phase {
            case .missing, .approvalRequired:
                Button("Review acquisition") {
                    store.showTutorialPrototypeApproval()
                }
                .accessibilityIdentifier("tutorialPrototype.dataset.review.\(dataset.id)")
            case .downloading, .verifying, .unpacking:
                Button("Cancel", role: .destructive) {
                    store.cancelTutorialPrototypeAcquisition()
                }
                .accessibilityIdentifier("tutorialPrototype.dataset.cancel.\(dataset.id)")
            case .cancelled:
                Button("Resume") { store.resumeTutorialPrototypeAcquisition() }
                    .accessibilityIdentifier("tutorialPrototype.dataset.resume.\(dataset.id)")
                Button("Restart") { store.restartTutorialPrototypeAcquisition() }
                    .accessibilityIdentifier("tutorialPrototype.dataset.restart.\(dataset.id)")
            case .offline, .checksumFailed, .unsafeArchive:
                Button("Retry") { store.retryTutorialPrototypeAcquisition() }
                    .accessibilityIdentifier("tutorialPrototype.dataset.retry.\(dataset.id)")
                Button("Restart") { store.restartTutorialPrototypeAcquisition() }
                    .accessibilityIdentifier("tutorialPrototype.dataset.restart.\(dataset.id)")
            case .diskFailed:
                Button("Make space available and retry") {
                    store.makeSpaceAndRetryTutorialPrototypeAcquisition()
                }
                .accessibilityIdentifier("tutorialPrototype.dataset.makeSpaceAvailable.\(dataset.id)")
            case .ready:
                Label("Verified and safely materialized", systemImage: "checkmark.circle.fill")
                    .workbenchFont(.caption, weight: .semibold)
                    .foregroundStyle(.green)
            }
            Spacer()
        }
    }

    @ViewBuilder
    private var taskParameterCard: some View {
        let isReady = tutorial?.dataset.isReady == true
        if isReady {
            Button {
                guard let taskID = tutorial?.fixtureTask.id else { return }
                store.openPrototypeTutorialTask(taskID: taskID)
            } label: {
                taskParameterCardContent(isReady: true)
            }
            .buttonStyle(.plain)
            .help("Open the task with these tutorial overrides")
            .accessibilityIdentifier("notebook.parameters.open.tutorial-task-twhya-imager")
        } else {
            taskParameterCardContent(isReady: false)
                .accessibilityElement(children: .combine)
                .accessibilityLabel("Create a TW Hya continuum image parameters")
                .accessibilityValue("Waiting for data")
                .accessibilityIdentifier("notebook.parameters.open.tutorial-task-twhya-imager")
        }
    }

    private func taskParameterCardContent(isReady: Bool) -> some View {
        VStack(alignment: .leading, spacing: 6) {
            HStack {
                Text("# Create a TW Hya continuum image")
                    .font(.system(size: 12, weight: .semibold, design: .monospaced))
                    .foregroundStyle(.primary)
                Spacer()
                Label(
                    isReady ? "Open Task" : "Waiting for data",
                    systemImage: isReady ? "arrow.up.forward.app" : "lock"
                )
                .workbenchFont(.caption)
                .foregroundStyle(.primary)
            }
            Text("[parameters]")
                .font(.system(size: 12, design: .monospaced))
                .foregroundStyle(.primary)
            ForEach(tutorial?.fixtureTask.parameterRows.prefix(5) ?? []) { row in
                Text("\(row.parameterID) = \(tomlValue(row.value))")
                    .font(.system(size: 12, design: .monospaced))
                    .foregroundStyle(.primary)
                    .lineLimit(1)
            }
        }
        .padding(11)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(Color.secondary.opacity(0.055))
        .clipShape(RoundedRectangle(cornerRadius: 5))
        .contentShape(Rectangle())
    }

    private var approvalPresented: Binding<Bool> {
        Binding(
            get: { tutorial?.activeApproval != nil },
            set: { presented in
                if !presented, tutorial?.activeApproval != nil {
                    store.dismissTutorialPrototypeApproval()
                }
            }
        )
    }

    private var approvalSheet: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 18) {
                VStack(alignment: .leading, spacing: 5) {
                    Label("Review dataset acquisition", systemImage: "arrow.down.circle")
                        .workbenchFont(.title2, weight: .semibold)
                    Text("Nothing downloads until you approve these exact fixture facts.")
                        .foregroundStyle(.secondary)
                }

                if let facts = tutorial?.activeApproval {
                    VStack(alignment: .leading, spacing: 10) {
                        approvalRow("Scheme", facts.scheme, id: "scheme")
                        approvalRow("Requested source", facts.requestedURL, id: "requestedSource")
                        approvalRow("Resolved source", facts.resolvedURL, id: "resolvedSource")
                        approvalRow(
                            "Redirects",
                            facts.redirects.isEmpty ? "None" : facts.redirects.joined(separator: " → "),
                            id: "redirects"
                        )
                        approvalRow(
                            "Expected size",
                            "\(formatBytes(facts.expectedSizeBytes)) (\(facts.expectedSizeBytes) bytes)",
                            id: "expectedSize"
                        )
                        approvalRow("Project destination", facts.destination, id: "destination")
                        approvalRow("SHA-256", facts.expectedSHA256, id: "checksum")
                        approvalRow(
                            "Disk requirement",
                            "\(formatBytes(facts.requiredDiskBytes)) required · \(formatBytes(facts.freeDiskBytes)) free",
                            id: "diskRequirement"
                        )
                        approvalRow("Extraction plan", facts.extractionPlan, id: "extractionPlan")
                    }

                    VStack(alignment: .leading, spacing: 7) {
                        Text("Optional verification checks")
                            .workbenchFont(.headline)
                        ForEach(facts.optionalChecks) { check in
                            Label(check.label, systemImage: check.isEnabled ? "checkmark.square" : "square")
                                .workbenchFont(.subheadline)
                        }
                    }
                }

                HStack {
                    Button("Cancel") { store.dismissTutorialPrototypeApproval() }
                        .accessibilityIdentifier("tutorialPrototype.approval.cancel")
                    Spacer()
                    Button("Approve and download") {
                        store.approveTutorialPrototypeAcquisition()
                    }
                    .buttonStyle(.borderedProminent)
                    .accessibilityIdentifier("tutorialPrototype.approval.approve")
                }
            }
            .padding(24)
        }
        .frame(minWidth: 680, minHeight: 610)
        .accessibilityIdentifier("tutorialPrototype.approval.sheet")
    }

    private func approvalRow(_ label: String, _ value: String, id: String) -> some View {
        VStack(alignment: .leading, spacing: 3) {
            Text(label)
                .workbenchFont(.caption, weight: .semibold)
                .foregroundStyle(.secondary)
                .accessibilityHidden(true)
            Text(value)
                .workbenchFont(.subheadline, design: id == "checksum" ? .monospaced : .default)
                .textSelection(.enabled)
                .fixedSize(horizontal: false, vertical: true)
                .accessibilityLabel(label)
                .accessibilityIdentifier("tutorialPrototype.approval.\(id)")
                .accessibilityValue(value)
        }
    }

    private var progressLabel: String {
        let sections = tutorial?.sections ?? []
        let completed = sections.filter { $0.status == .completed }.count
        return "\(completed) of \(sections.count) sections"
    }

    private func formatBytes(_ bytes: UInt64) -> String {
        ByteCountFormatter.string(fromByteCount: Int64(bytes), countStyle: .file)
    }

    private func tomlValue(_ value: String) -> String {
        if Double(value) != nil || value == "true" || value == "false" {
            return value
        }
        return "\"\(value)\""
    }
}

private extension TutorialNotebookSectionStatus {
    var color: Color {
        switch self {
        case .notStarted: .secondary
        case .inProgress: .accentColor
        case .completed: .green
        case .blocked: .secondary.opacity(0.55)
        }
    }
}

private extension TutorialNotebookAcquisitionPhase {
    var label: String {
        switch self {
        case .missing: "Missing"
        case .approvalRequired: "Approval required"
        case .downloading: "Downloading"
        case .verifying: "Verifying"
        case .unpacking: "Unpacking"
        case .ready: "Ready"
        case .cancelled: "Cancelled"
        case .checksumFailed: "Checksum failed"
        case .diskFailed: "Insufficient disk"
        case .offline: "Offline"
        case .unsafeArchive: "Unsafe archive"
        }
    }

    var icon: String {
        switch self {
        case .missing, .approvalRequired: "arrow.down.circle"
        case .downloading: "arrow.down.circle.fill"
        case .verifying: "checkmark.shield"
        case .unpacking: "shippingbox"
        case .ready: "checkmark.circle.fill"
        case .cancelled: "pause.circle"
        case .checksumFailed, .diskFailed, .offline, .unsafeArchive: "exclamationmark.triangle.fill"
        }
    }

    var color: Color {
        switch self {
        case .ready: .green
        case .downloading, .verifying, .unpacking: .accentColor
        case .checksumFailed, .diskFailed, .offline, .unsafeArchive: .red
        case .cancelled: .orange
        case .missing, .approvalRequired: .primary
        }
    }

    var isFailure: Bool {
        switch self {
        case .checksumFailed, .diskFailed, .offline, .unsafeArchive: true
        default: false
        }
    }
}
