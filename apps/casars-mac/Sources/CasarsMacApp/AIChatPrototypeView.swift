import CasarsMacCore
import AppKit
import SwiftUI

enum AIChatPrototypeLayout {
    case drawer
    case expanded
}

struct AIChatPrototypeView: View {
    @ObservedObject var store: WorkbenchStore
    let layout: AIChatPrototypeLayout

    @State private var selectedCitationID: String?
    @State private var sourceCitation: PrototypeAICitation?
    @State private var messageForPin: PrototypeAIMessage?
    @State private var contextOpen = false

    private var projection: PrototypeAIChatProjection? {
        store.state.prototypeAI
    }

    var body: some View {
        ZStack(alignment: .bottomTrailing) {
            VStack(spacing: 0) {
                if let projection {
                    header(projection)
                    Divider()
                    conversation(projection)
                    Divider()
                    composer(projection)
                } else {
                    Text("AI prototype fixture unavailable")
                        .foregroundStyle(.secondary)
                }
            }

            if contextOpen, let projection {
                contextPanel(projection)
                    .padding(.trailing, 10)
                    .padding(.bottom, 112)
            }
        }
        .background(Color(nsColor: .textBackgroundColor))
        .sheet(item: $sourceCitation) { citation in
            sourceSheet(citation)
        }
        .sheet(item: $messageForPin) { message in
            pinSheet(message)
        }
    }

    private func header(_ projection: PrototypeAIChatProjection) -> some View {
        VStack(alignment: .leading, spacing: 7) {
            HStack(spacing: 8) {
                Image(systemName: "sparkles")
                    .foregroundStyle(.tint)

                VStack(alignment: .leading, spacing: 1) {
                    Text(layout == .drawer ? "Notebook chat" : "AI · TW Hya discussion")
                        .workbenchFont(.headline)
                        .accessibilityIdentifier(
                            layout == .drawer ? "aiPrototype.drawer" : "aiPrototype.expanded"
                        )
                    Text("Attached to \(projection.primaryAttachment)")
                        .workbenchFont(.caption)
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                        .accessibilityIdentifier("aiPrototype.attachment")
                }

                Spacer()

                Menu {
                    Button("TW Hya continuum", systemImage: "checkmark") {}
                    Button("Imaging strategy") {}
                    Divider()
                    Button("New fixture discussion", systemImage: "plus") {
                        store.setAIPrototypeDraft("")
                    }
                } label: {
                    Image(systemName: "clock")
                }
                .menuStyle(.borderlessButton)
                .help("Conversation history")
                .accessibilityLabel("Conversation history")
                .accessibilityIdentifier("aiPrototype.history")

                if layout == .drawer {
                    Button {
                        store.expandAIPrototypeConversation()
                    } label: {
                        Image(systemName: "arrow.up.left.and.arrow.down.right")
                    }
                    .buttonStyle(.borderless)
                    .help("Open in AI tab")
                    .accessibilityLabel("Open in AI tab")
                    .accessibilityIdentifier("aiPrototype.expand")

                    Button {
                        store.closeAIPrototypeConversation()
                    } label: {
                        Image(systemName: "xmark")
                    }
                    .buttonStyle(.borderless)
                    .help("Close chat")
                    .accessibilityLabel("Close chat")
                    .accessibilityIdentifier("aiPrototype.closeDrawer")
                } else {
                    Button("Dock beside notebook") {
                        store.dockAIPrototypeConversation()
                    }
                    .controlSize(.small)
                    .accessibilityIdentifier("aiPrototype.dock")
                }
            }

            HStack(spacing: 6) {
                Menu {
                    ForEach(projection.providers) { provider in
                        Button(provider.label) {
                            store.selectAIPrototypeProvider(provider.id)
                        }
                    }
                    Divider()
                    ForEach(projection.selectedProvider?.models ?? [], id: \.self) { model in
                        Button(model) {
                            store.selectAIPrototypeModel(model)
                        }
                    }
                } label: {
                    Text("\(projection.selectedProvider?.label ?? "Provider") · \(projection.selectedModel)")
                        .lineLimit(1)
                }
                .menuStyle(.borderlessButton)
                .workbenchFont(.caption)
                .accessibilityIdentifier("aiPrototype.provider")

                Spacer()

                HStack(spacing: 4) {
                    Image(systemName: "lock.shield")
                    Text("Fixture · \(store.prototypeProductionBoundaryInvocationCount) calls")
                }
                .workbenchFont(.caption2)
                .foregroundStyle(.secondary)
                .accessibilityElement(children: .ignore)
                .accessibilityLabel(
                    "Fixture · \(store.prototypeProductionBoundaryInvocationCount) production calls"
                )
                .accessibilityValue("\(store.prototypeProductionBoundaryInvocationCount)")
                .accessibilityIdentifier("aiPrototype.boundaryStatus")
            }
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 10)
        .background(.bar)
    }

    private func conversation(_ projection: PrototypeAIChatProjection) -> some View {
        ScrollView {
            LazyVStack(alignment: .leading, spacing: 12) {
                if projection.messages.isEmpty {
                    emptyConversation(projection)
                } else {
                    ForEach(projection.messages) { message in
                        messageRow(message, projection: projection)
                    }
                }

                if projection.responseState == .streaming {
                    HStack(spacing: 8) {
                        ProgressView().controlSize(.small)
                        Text("Reading open-tab state and local sources…")
                            .foregroundStyle(.secondary)
                    }
                    .accessibilityIdentifier("aiPrototype.response.streaming")
                }

                responseNotice(projection)

                if projection.messages.contains(where: { $0.role == .assistant }) {
                    notebookSuggestionSummary(projection)
                }
            }
            .frame(maxWidth: layout == .expanded ? 760 : .infinity, alignment: .leading)
            .padding(layout == .expanded ? 24 : 12)
            .frame(maxWidth: .infinity)
        }
        .accessibilityIdentifier("aiPrototype.conversationScroll")
    }

    private func notebookSuggestionSummary(_ projection: PrototypeAIChatProjection) -> some View {
        let unresolvedCount = projection.proposals.filter { proposal in
            proposal.state == .pending || proposal.state == .running
        }.count

        return Button {
            store.showAIPrototypeNotebookSuggestions()
        } label: {
            HStack(spacing: 9) {
                Image(systemName: "doc.badge.ellipsis")
                    .foregroundStyle(.tint)
                VStack(alignment: .leading, spacing: 2) {
                    Text("Suggestions are in Analysis.md")
                        .workbenchFont(.subheadline, weight: .semibold)
                    Text("\(unresolvedCount) pending · review each change where it will be applied")
                        .workbenchFont(.caption)
                        .foregroundStyle(.secondary)
                }
                Spacer()
                Image(systemName: "arrow.up.forward.app")
                    .foregroundStyle(.secondary)
            }
            .padding(10)
            .contentShape(Rectangle())
        }
        .buttonStyle(.plain)
        .background(Color.accentColor.opacity(0.06))
        .clipShape(RoundedRectangle(cornerRadius: 8))
        .accessibilityIdentifier("aiPrototype.openNotebookSuggestions")
    }

    private func emptyConversation(_ projection: PrototypeAIChatProjection) -> some View {
        VStack(alignment: .leading, spacing: 12) {
            Label("Ask about this project", systemImage: "bubble.left.and.text.bubble.right")
                .workbenchFont(.title3, weight: .semibold)
            Text("I can inspect every open tab, retrieve from the radio astronomy corpus, and explain the current CASA-RS source. Nothing is sent until you ask.")
                .foregroundStyle(.secondary)

            Text("Try asking")
                .workbenchFont(.caption, weight: .semibold)
                .foregroundStyle(.secondary)

            ForEach(projection.suggestedPrompts, id: \.self) { prompt in
                Button(prompt) {
                    store.setAIPrototypeDraft(prompt)
                }
                .buttonStyle(.link)
                .multilineTextAlignment(.leading)
                .accessibilityIdentifier("aiPrototype.suggestion.\(suggestionID(prompt))")
            }
        }
        .padding(14)
        .background(Color.accentColor.opacity(0.06))
        .clipShape(RoundedRectangle(cornerRadius: 10))
    }

    private func messageRow(
        _ message: PrototypeAIMessage,
        projection: PrototypeAIChatProjection
    ) -> some View {
        HStack {
            if message.role == .user { Spacer(minLength: layout == .expanded ? 120 : 28) }

            VStack(alignment: .leading, spacing: 7) {
                HStack {
                    Text(message.role == .user ? "You" : "Assistant")
                        .workbenchFont(.caption, weight: .semibold)
                        .foregroundStyle(.secondary)
                        .accessibilityIdentifier("aiPrototype.message.\(message.id)")
                    if let provider = message.providerLabel, let model = message.modelLabel {
                        Text("\(provider) · \(model)")
                            .workbenchFont(.caption2)
                            .foregroundStyle(.tertiary)
                            .lineLimit(1)
                    }
                    Spacer()
                    if message.role == .assistant {
                        Button(message.pinned ? "Pinned" : "Pin") {
                            messageForPin = message
                        }
                        .buttonStyle(.link)
                        .disabled(message.pinned)
                        .accessibilityIdentifier("aiPrototype.message.\(message.id).pin")
                    }
                }

                Text(message.text)
                    .textSelection(.enabled)

                if !message.citations.isEmpty {
                    HStack(spacing: 6) {
                        ForEach(Array(message.citations.enumerated()), id: \.element.id) { index, citation in
                            Button("[\(index + 1)] \(citation.label)") {
                                selectedCitationID = selectedCitationID == citation.id ? nil : citation.id
                            }
                            .buttonStyle(.bordered)
                            .controlSize(.small)
                            .accessibilityIdentifier("aiPrototype.citation.\(citation.id)")
                        }
                    }

                    DisclosureGroup("Sent context") {
                        Text(contextLabels(message.usedContextIDs, projection: projection))
                            .workbenchFont(.caption)
                            .foregroundStyle(.secondary)
                    }
                    .workbenchFont(.caption)
                    .accessibilityIdentifier("aiPrototype.message.\(message.id).usedContext")
                }

                if let citation = selectedCitation(in: message) {
                    VStack(alignment: .leading, spacing: 4) {
                        Text(citation.locator)
                            .workbenchFont(.caption, weight: .semibold)
                        Text(citation.excerpt)
                            .workbenchFont(.caption)
                        Button("Open source preview") {
                            sourceCitation = citation
                        }
                        .buttonStyle(.link)
                        .accessibilityIdentifier("aiPrototype.citation.openSource")
                    }
                    .padding(8)
                    .background(Color.accentColor.opacity(0.08))
                    .clipShape(RoundedRectangle(cornerRadius: 7))
                    .accessibilityIdentifier("aiPrototype.sourcePreview")
                }
            }
            .padding(11)
            .background(message.role == .user ? Color.accentColor.opacity(0.12) : Color.secondary.opacity(0.08))
            .clipShape(RoundedRectangle(cornerRadius: 10))

            if message.role == .assistant { Spacer(minLength: layout == .expanded ? 70 : 0) }
        }
    }

    @ViewBuilder
    private func responseNotice(_ projection: PrototypeAIChatProjection) -> some View {
        switch projection.responseState {
        case .rateLimited, .offline, .failed:
            HStack {
                Label(responseFailureLabel(projection.responseState), systemImage: "exclamationmark.triangle")
                    .foregroundStyle(.orange)
                    .accessibilityIdentifier("aiPrototype.response.error")
                Spacer()
                Button("Retry") { store.retryAIPrototypeResponse() }
                    .accessibilityIdentifier("aiPrototype.response.retry")
            }
            .padding(9)
            .background(Color.orange.opacity(0.08))
            .clipShape(RoundedRectangle(cornerRadius: 7))
        case .cancelled:
            Text("Response cancelled. It was not retried automatically.")
                .workbenchFont(.caption)
                .foregroundStyle(.secondary)
        case .restartRequired:
            HStack {
                Text("Worker did not respond; restart is explicit.")
                    .accessibilityIdentifier("aiPrototype.response.restartRequired")
                Spacer()
                Button("Restart") { store.restartAIPrototypeWorker() }
                    .accessibilityIdentifier("aiPrototype.response.restart")
            }
            .foregroundStyle(.orange)
        default:
            EmptyView()
        }
    }

    private func composer(_ projection: PrototypeAIChatProjection) -> some View {
        VStack(alignment: .leading, spacing: 8) {
            contextDisclosure(projection)

            HStack(alignment: .bottom, spacing: 8) {
                ZStack(alignment: .topLeading) {
                    if projection.draft.isEmpty {
                        Text("Ask anything about this project…")
                            .foregroundStyle(.tertiary)
                            .padding(.horizontal, 6)
                            .padding(.vertical, 8)
                    }
                    PrototypeAIComposerEditor(text: Binding(
                        get: { projection.draft },
                        set: { store.setAIPrototypeDraft($0) }
                    ))
                }
                .frame(minHeight: 58, maxHeight: layout == .expanded ? 100 : 74)
                .background(Color(nsColor: .controlBackgroundColor))
                .clipShape(RoundedRectangle(cornerRadius: 8))
                .overlay(RoundedRectangle(cornerRadius: 8).stroke(Color.secondary.opacity(0.24)))

                if projection.responseState == .streaming {
                    Button("Cancel") { store.cancelAIPrototypeResponse() }
                        .accessibilityIdentifier("aiPrototype.response.cancel")
                } else {
                    Button {
                        store.sendAIPrototypePrompt(projection.draft)
                    } label: {
                        Image(systemName: "arrow.up")
                    }
                    .buttonStyle(.borderedProminent)
                    .disabled(
                        projection.draft.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
                            || projection.corpusState != .ready
                    )
                    .help("Send")
                    .accessibilityLabel("Send")
                    .accessibilityIdentifier("aiPrototype.send")
                }
            }
        }
        .padding(10)
        .background(.bar)
    }

    private func contextDisclosure(_ projection: PrototypeAIChatProjection) -> some View {
        Button {
            contextOpen.toggle()
        } label: {
            HStack(spacing: 5) {
                Image(systemName: "chevron.right")
                    .rotationEffect(.degrees(contextOpen ? 90 : 0))
                Text("Context: \(projection.selectedContexts.count) items · 86 KB → \(projection.selectedProvider?.label ?? "provider")")
                    .lineLimit(1)
                Spacer(minLength: 0)
            }
            .contentShape(Rectangle())
        }
        .buttonStyle(.plain)
        .workbenchFont(.caption)
        .accessibilityIdentifier("aiPrototype.egressPreview")
    }

    private func contextPanel(_ projection: PrototypeAIChatProjection) -> some View {
        VStack(spacing: 0) {
            HStack {
                Text("Context for the next turn")
                    .workbenchFont(.headline)
                Spacer()
                Button {
                    contextOpen = false
                } label: {
                    Image(systemName: "xmark")
                }
                .buttonStyle(.borderless)
                .accessibilityLabel("Close context")
                .accessibilityIdentifier("aiPrototype.context.close")
            }
            .padding(12)

            Divider()

            ScrollView {
                contextDetails(projection)
                    .padding(14)
            }
        }
        .frame(width: layout == .drawer ? 370 : 400, height: 500)
        .background(Color(nsColor: .windowBackgroundColor))
        .clipShape(RoundedRectangle(cornerRadius: 10))
        .overlay(RoundedRectangle(cornerRadius: 10).stroke(Color.secondary.opacity(0.3)))
        .shadow(radius: 12)
    }

    private func contextDetails(_ projection: PrototypeAIChatProjection) -> some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("Automatic read-only workspace awareness")
                .workbenchFont(.caption, weight: .semibold)
            ForEach(projection.workspaceSources) { source in
                HStack(alignment: .top, spacing: 6) {
                    Image(systemName: source.openTab ? "rectangle.on.rectangle" : "books.vertical")
                        .foregroundStyle(.secondary)
                    VStack(alignment: .leading, spacing: 1) {
                        Text(source.label).workbenchFont(.caption, weight: .semibold)
                        Text(source.detail).workbenchFont(.caption2).foregroundStyle(.secondary)
                    }
                }
                .accessibilityIdentifier("aiPrototype.workspaceSource.\(source.id)")
            }

            Divider()
            Text("Provider payload for the next turn")
                .workbenchFont(.caption, weight: .semibold)
            ForEach(projection.contexts) { context in
                Toggle(isOn: Binding(
                    get: { context.selected },
                    set: { _ in store.toggleAIPrototypeContext(context.id) }
                )) {
                    VStack(alignment: .leading, spacing: 1) {
                        Text(context.label).workbenchFont(.caption)
                        Text(context.egressSummary).workbenchFont(.caption2).foregroundStyle(.secondary)
                    }
                }
                .toggleStyle(.checkbox)
                .accessibilityIdentifier("aiPrototype.context.\(context.id)")
            }

            corpusControl(projection)
            Text("Secrets, raw visibilities, and bulk arrays remain local.")
                .workbenchFont(.caption2)
                .foregroundStyle(.secondary)
        }
    }

    private func corpusControl(_ projection: PrototypeAIChatProjection) -> some View {
        HStack(spacing: 7) {
            Circle()
                .fill(activityColor(projection.corpusState))
                .frame(width: 7, height: 7)
            Text(corpusLabel(projection.corpusState))
                .workbenchFont(.caption)
                .foregroundStyle(.secondary)
                .accessibilityIdentifier("aiPrototype.corpusState")
            Spacer()
            if projection.corpusState == .indexing {
                ProgressView().controlSize(.small)
                Button("Cancel") { store.cancelAIPrototypeIndexing() }
                    .buttonStyle(.link)
                    .accessibilityIdentifier("aiPrototype.index.cancel")
            } else {
                Button(projection.corpusState == .ready ? "Re-index" : "Retry") {
                    store.startAIPrototypeIndexing()
                }
                .buttonStyle(.link)
                .accessibilityIdentifier("aiPrototype.index.start")
            }
        }
    }

    private func sourceSheet(_ citation: PrototypeAICitation) -> some View {
        VStack(alignment: .leading, spacing: 14) {
            Label("Fixture source preview", systemImage: "doc.text.magnifyingglass")
                .workbenchFont(.title2, weight: .semibold)
                .accessibilityIdentifier("aiPrototype.sourceSheet")
            Text(citation.label).workbenchFont(.headline)
            Text(citation.locator)
                .workbenchFont(.body, design: .monospaced)
            Text(citation.excerpt)
                .textSelection(.enabled)
            Spacer()
            HStack {
                Spacer()
                Button("Done") { sourceCitation = nil }
                    .keyboardShortcut(.defaultAction)
            }
        }
        .padding(24)
        .frame(width: 620, height: 360)
    }

    private func pinSheet(_ message: PrototypeAIMessage) -> some View {
        PrototypeAIPinSheet(store: store, message: message) {
            messageForPin = nil
        }
    }

    private func selectedCitation(in message: PrototypeAIMessage) -> PrototypeAICitation? {
        guard let selectedCitationID else { return nil }
        return message.citations.first { $0.id == selectedCitationID }
    }

    private func contextLabels(
        _ ids: [String],
        projection: PrototypeAIChatProjection
    ) -> String {
        let labels = ids.compactMap { id in projection.contexts.first { $0.id == id }?.label }
        return labels.joined(separator: " · ")
    }

    private func corpusLabel(_ state: PrototypeAIActivityState) -> String {
        switch state {
        case .ready: "Corpus ready · 4,814 documents + CASA-RS source"
        case .indexing: "Indexing local sources"
        case .offline: "Corpus offline"
        case .cancelled: "Indexing cancelled"
        case .failed: "Indexing failed"
        default: state.rawValue.capitalized
        }
    }

    private func responseFailureLabel(_ state: PrototypeAIActivityState) -> String {
        switch state {
        case .rateLimited: "Provider rate limited; nothing was retried."
        case .offline: "Provider offline; project context remains local."
        default: "Provider failed before returning an answer."
        }
    }

    private func activityColor(_ state: PrototypeAIActivityState) -> Color {
        switch state {
        case .ready, .completed: .green
        case .indexing, .streaming: .blue
        case .offline, .failed, .rateLimited, .restartRequired: .orange
        case .cancelled: .secondary
        default: .secondary
        }
    }

    private func suggestionID(_ prompt: String) -> String {
        if prompt.contains("current plot") { return "plot" }
        if prompt.contains("Imager") { return "task" }
        return "data-types"
    }
}

private struct PrototypeAIComposerEditor: NSViewRepresentable {
    @Binding var text: String

    func makeCoordinator() -> Coordinator {
        Coordinator(text: $text)
    }

    func makeNSView(context: Context) -> NSScrollView {
        let textView = UserActivatedTextView()
        textView.delegate = context.coordinator
        textView.string = text
        textView.isRichText = false
        textView.isEditable = true
        textView.isSelectable = true
        textView.drawsBackground = false
        textView.font = .preferredFont(forTextStyle: .body)
        textView.textContainerInset = NSSize(width: 5, height: 6)
        textView.isVerticallyResizable = true
        textView.isHorizontallyResizable = false
        textView.autoresizingMask = [.width]
        textView.textContainer?.widthTracksTextView = true
        textView.isAutomaticTextCompletionEnabled = false
        textView.isAutomaticSpellingCorrectionEnabled = false
        textView.isAutomaticTextReplacementEnabled = false
        textView.isAutomaticQuoteSubstitutionEnabled = false
        textView.isAutomaticDashSubstitutionEnabled = false
        textView.setAccessibilityLabel("Ask anything about this project")
        textView.setAccessibilityIdentifier("aiPrototype.input")

        let scrollView = NSScrollView()
        scrollView.drawsBackground = false
        scrollView.hasVerticalScroller = true
        scrollView.autohidesScrollers = true
        scrollView.borderType = .noBorder
        scrollView.documentView = textView
        return scrollView
    }

    func updateNSView(_ scrollView: NSScrollView, context: Context) {
        guard let textView = scrollView.documentView as? UserActivatedTextView,
              textView.string != text
        else { return }
        textView.string = text
    }

    final class Coordinator: NSObject, NSTextViewDelegate {
        private var text: Binding<String>

        init(text: Binding<String>) {
            self.text = text
        }

        func textDidChange(_ notification: Notification) {
            guard let textView = notification.object as? NSTextView else { return }
            text.wrappedValue = textView.string
        }
    }
}

/// Prevents AppKit from opening its completion UI merely because the composer
/// appeared. A real click enables ordinary keyboard focus and editing.
private final class UserActivatedTextView: NSTextView {
    private var userRequestedFocus = false

    override var acceptsFirstResponder: Bool {
        userRequestedFocus
    }

    override func mouseDown(with event: NSEvent) {
        userRequestedFocus = true
        super.mouseDown(with: event)
    }
}

private struct PrototypeAIPinSheet: View {
    @ObservedObject var store: WorkbenchStore
    let message: PrototypeAIMessage
    let dismiss: () -> Void

    @State private var representation = "Markdown conclusion"
    @State private var insertionTarget = "After current heading"

    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
            Label("Pin to notebook", systemImage: "pin")
                .workbenchFont(.title2, weight: .semibold)
                .accessibilityIdentifier("aiPrototype.pinSheet")
            Picker("Representation", selection: $representation) {
                Text("Markdown conclusion").tag("Markdown conclusion")
                Text("Cited note block").tag("Cited note block")
                Text("Conversation link").tag("Conversation link")
            }
            Picker("Insert", selection: $insertionTarget) {
                Text("After current heading").tag("After current heading")
                Text("End of notebook").tag("End of notebook")
            }

            GroupBox("Insertion preview") {
                VStack(alignment: .leading, spacing: 6) {
                    Text(message.text)
                    Text("Citations and conversation provenance retained.")
                        .workbenchFont(.caption)
                        .foregroundStyle(.secondary)
                }
                .frame(maxWidth: .infinity, alignment: .leading)
            }

            Spacer()
            HStack {
                Button("Cancel", action: dismiss)
                Spacer()
                Button("Confirm pin") {
                    store.pinAIPrototypeMessage(message.id)
                    dismiss()
                }
                .buttonStyle(.borderedProminent)
                .accessibilityIdentifier("aiPrototype.pin.confirm")
            }
        }
        .padding(24)
        .frame(width: 620, height: 430)
    }
}
