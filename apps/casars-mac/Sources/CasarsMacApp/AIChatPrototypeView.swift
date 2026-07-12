import CasarsMacCore
import SwiftUI

struct AIChatPrototypeView: View {
    @ObservedObject var store: WorkbenchStore
    @State private var draft = "Compare the current plot with the paper and suggest a safe next step."
    @State private var selectedCitationID: String?
    @FocusState private var composerFocused: Bool

    private var projection: PrototypeAIChatProjection? {
        store.state.prototypeAI
    }

    var body: some View {
        VStack(spacing: 0) {
            if let projection {
                header(projection)
                Divider()
                contextBar(projection)
                Divider()
                conversation(projection)
                Divider()
                composer(projection)
            } else {
                Text("AI prototype fixture unavailable")
                    .foregroundStyle(.secondary)
            }
        }
        .onAppear {
            DispatchQueue.main.async { composerFocused = false }
        }
    }

    private func header(_ projection: PrototypeAIChatProjection) -> some View {
        HStack(spacing: 14) {
            VStack(alignment: .leading, spacing: 2) {
                Text("Project discussion")
                    .workbenchFont(.headline)
                Text("Cited answers and explicitly approved actions")
                    .workbenchFont(.caption)
                    .foregroundStyle(.secondary)
            }

            Spacer()

            Picker("Provider", selection: Binding(
                get: { projection.selectedProviderID },
                set: { store.selectAIPrototypeProvider($0) }
            )) {
                ForEach(projection.providers) { provider in
                    Text(provider.label).tag(provider.id)
                }
            }
            .labelsHidden()
            .frame(width: 180)
            .accessibilityLabel("AI provider")
            .accessibilityIdentifier("aiPrototype.provider")

            Picker("Model", selection: Binding(
                get: { projection.selectedModel },
                set: { store.selectAIPrototypeModel($0) }
            )) {
                ForEach(projection.selectedProvider?.models ?? [], id: \.self) { model in
                    Text(model).tag(model)
                }
            }
            .labelsHidden()
            .frame(width: 130)
            .accessibilityLabel("AI model")
            .accessibilityIdentifier("aiPrototype.model")

            Label("Fixture · \(store.prototypeProductionBoundaryInvocationCount) production calls", systemImage: "lock.shield")
                .workbenchFont(.caption)
                .foregroundStyle(.secondary)
                .accessibilityValue("\(store.prototypeProductionBoundaryInvocationCount)")
                .accessibilityIdentifier("aiPrototype.boundaryStatus")
        }
        .padding(.horizontal, 18)
        .padding(.vertical, 10)
    }

    private func contextBar(_ projection: PrototypeAIChatProjection) -> some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack(spacing: 7) {
                Text("Context")
                    .workbenchFont(.caption, weight: .semibold)
                    .foregroundStyle(.secondary)

                ForEach(projection.contexts) { context in
                    Button {
                        store.toggleAIPrototypeContext(context.id)
                    } label: {
                        HStack(spacing: 4) {
                            Image(systemName: context.selected ? "checkmark.circle.fill" : "circle")
                            Text(context.label)
                        }
                        .workbenchFont(.caption)
                    }
                    .buttonStyle(.bordered)
                    .tint(context.selected ? .accentColor : .secondary)
                    .help(context.detail)
                    .accessibilityIdentifier("aiPrototype.context.\(context.id)")
                }

                Spacer()
                corpusControl(projection)
            }

            DisclosureGroup("Preview what leaves this Mac") {
                VStack(alignment: .leading, spacing: 3) {
                    ForEach(projection.selectedContexts) { context in
                        Text("• \(context.label): \(context.egressSummary)")
                    }
                    Text("Secrets, raw visibilities, and unselected context remain local.")
                        .foregroundStyle(.secondary)
                }
                .workbenchFont(.caption)
                .padding(.top, 4)
            }
            .workbenchFont(.caption)
            .accessibilityIdentifier("aiPrototype.egressPreview")
        }
        .padding(.horizontal, 18)
        .padding(.vertical, 8)
    }

    @ViewBuilder
    private func corpusControl(_ projection: PrototypeAIChatProjection) -> some View {
        HStack(spacing: 7) {
            Circle()
                .fill(activityColor(projection.corpusState))
                .frame(width: 7, height: 7)
            Text(corpusLabel(projection.corpusState))
                .workbenchFont(.caption)
                .foregroundStyle(.secondary)
                .accessibilityIdentifier("aiPrototype.corpusState")

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

    private func conversation(_ projection: PrototypeAIChatProjection) -> some View {
        ScrollView {
            LazyVStack(alignment: .leading, spacing: 12) {
                ForEach(projection.messages) { message in
                    messageRow(message, projection: projection)
                }

                if projection.responseState == .streaming {
                    HStack(spacing: 8) {
                        ProgressView().controlSize(.small)
                        Text("Reading approved context and composing a cited answer…")
                            .foregroundStyle(.secondary)
                    }
                    .accessibilityIdentifier("aiPrototype.response.streaming")
                }

                responseNotice(projection)

                Text("Proposed actions")
                    .workbenchFont(.subheadline, weight: .semibold)
                    .padding(.top, 2)

                ScrollView(.horizontal, showsIndicators: false) {
                    HStack(alignment: .top, spacing: 10) {
                        ForEach(projection.proposals) { proposal in
                            proposalCard(proposal)
                                .frame(width: 320)
                        }
                    }
                }
            }
            .frame(maxWidth: 920, alignment: .leading)
            .padding(18)
            .frame(maxWidth: .infinity)
        }
    }

    private func messageRow(
        _ message: PrototypeAIMessage,
        projection: PrototypeAIChatProjection
    ) -> some View {
        VStack(alignment: .leading, spacing: 7) {
            HStack {
                Text(message.role == .user ? "You" : "Assistant")
                    .workbenchFont(.caption, weight: .semibold)
                    .foregroundStyle(.secondary)
                    .accessibilityIdentifier("aiPrototype.message.\(message.id)")
                if let provider = message.providerLabel, let model = message.modelLabel {
                    Text("\(provider) · \(model)")
                        .workbenchFont(.caption)
                        .foregroundStyle(.tertiary)
                }
                Spacer()
                if message.role == .assistant {
                    Button(message.pinned ? "Pinned" : "Pin to notebook") {
                        store.pinAIPrototypeMessage(message.id)
                    }
                    .buttonStyle(.link)
                    .disabled(message.pinned)
                    .accessibilityIdentifier("aiPrototype.message.\(message.id).pin")
                }
            }

            Text(message.text)
                .textSelection(.enabled)

            if !message.citations.isEmpty {
                HStack(spacing: 7) {
                    ForEach(message.citations) { citation in
                        Button(citation.label) {
                            selectedCitationID = selectedCitationID == citation.id ? nil : citation.id
                        }
                        .buttonStyle(.bordered)
                        .controlSize(.small)
                        .accessibilityIdentifier("aiPrototype.citation.\(citation.id)")
                    }
                    DisclosureGroup("Used context") {
                        Text(contextLabels(message.usedContextIDs, projection: projection))
                            .workbenchFont(.caption)
                            .foregroundStyle(.secondary)
                    }
                    .workbenchFont(.caption)
                    .accessibilityIdentifier("aiPrototype.message.\(message.id).usedContext")
                }
            }

            if let citation = selectedCitation(in: message) {
                VStack(alignment: .leading, spacing: 3) {
                    Text(citation.locator)
                        .workbenchFont(.caption, weight: .semibold)
                    Text(citation.excerpt)
                        .workbenchFont(.caption)
                    Button("Open source") {}
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
        .background(message.role == .assistant ? Color.secondary.opacity(0.08) : Color.accentColor.opacity(0.08))
        .clipShape(RoundedRectangle(cornerRadius: 9))
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
                Text("Worker did not respond; it must be restarted explicitly.")
                    .accessibilityIdentifier("aiPrototype.response.restartRequired")
                Spacer()
                Button("Restart worker") { store.restartAIPrototypeWorker() }
                    .accessibilityIdentifier("aiPrototype.response.restart")
            }
            .foregroundStyle(.orange)
        default:
            EmptyView()
        }
    }

    private func proposalCard(_ proposal: PrototypeAIProposal) -> some View {
        VStack(alignment: .leading, spacing: 7) {
            HStack {
                Label(proposal.kind.label, systemImage: proposalIcon(proposal.kind))
                    .workbenchFont(.caption, weight: .semibold)
                    .foregroundStyle(.secondary)
                Spacer()
                Text(proposal.state.rawValue.capitalized)
                    .workbenchFont(.caption)
                    .foregroundStyle(proposalStateColor(proposal.state))
                    .accessibilityIdentifier("aiPrototype.proposal.\(proposal.id).state")
            }
            Text(proposal.title)
                .workbenchFont(.subheadline, weight: .semibold)
            Text(proposal.summary)
                .workbenchFont(.caption)
                .foregroundStyle(.secondary)
                .lineLimit(2)

            DisclosureGroup("Review exact action") {
                VStack(alignment: .leading, spacing: 5) {
                    Text(proposal.exactPayload)
                        .workbenchFont(.caption, design: .monospaced)
                        .textSelection(.enabled)
                    Text(proposal.authority)
                        .workbenchFont(.caption, weight: .semibold)
                    ForEach(proposal.affectedPaths, id: \.self) { path in
                        Text(path)
                            .workbenchFont(.caption, design: .monospaced)
                    }
                }
                .padding(.top, 4)
            }
            .workbenchFont(.caption)
            .accessibilityIdentifier("aiPrototype.proposal.\(proposal.id).review")

            if let result = proposal.result {
                Text(result)
                    .workbenchFont(.caption)
                    .foregroundStyle(proposal.state == .failed ? .orange : .secondary)
                    .lineLimit(2)
            }

            Spacer(minLength: 0)
            proposalActions(proposal)
        }
        .padding(11)
        .frame(minHeight: 178, alignment: .topLeading)
        .background(.regularMaterial)
        .clipShape(RoundedRectangle(cornerRadius: 9))
        .overlay(RoundedRectangle(cornerRadius: 9).stroke(Color.secondary.opacity(0.18)))
    }

    @ViewBuilder
    private func proposalActions(_ proposal: PrototypeAIProposal) -> some View {
        switch proposal.state {
        case .pending:
            HStack {
                Button("Apply") { store.approveAIPrototypeProposal(proposal.id) }
                    .buttonStyle(.borderedProminent)
                    .controlSize(.small)
                    .accessibilityIdentifier("aiPrototype.proposal.\(proposal.id).apply")
                Button("Reject") { store.rejectAIPrototypeProposal(proposal.id) }
                    .controlSize(.small)
                    .accessibilityIdentifier("aiPrototype.proposal.\(proposal.id).reject")
            }
        case .running:
            HStack {
                ProgressView().controlSize(.small)
                Text("Running fixture…").workbenchFont(.caption)
                Spacer()
                Button("Cancel") { store.cancelAIPrototypeProposal(proposal.id) }
                    .controlSize(.small)
                    .accessibilityIdentifier("aiPrototype.proposal.\(proposal.id).cancel")
            }
        case .failed, .cancelled:
            Button("Review and retry") { store.retryAIPrototypeProposal(proposal.id) }
                .controlSize(.small)
                .accessibilityIdentifier("aiPrototype.proposal.\(proposal.id).retry")
        case .succeeded, .rejected:
            EmptyView()
        }
    }

    private func composer(_ projection: PrototypeAIChatProjection) -> some View {
        HStack(spacing: 9) {
            TextField("Ask about this project", text: $draft)
                .textFieldStyle(.roundedBorder)
                .focused($composerFocused)
                .onSubmit { sendDraft() }
                .disabled(projection.responseState == .streaming || projection.corpusState != .ready)
                .accessibilityIdentifier("aiPrototype.input")

            if projection.responseState == .streaming {
                Button("Cancel") { store.cancelAIPrototypeResponse() }
                    .accessibilityIdentifier("aiPrototype.response.cancel")
            } else {
                Button("Send") { sendDraft() }
                    .buttonStyle(.borderedProminent)
                    .disabled(draft.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty || projection.corpusState != .ready)
                    .accessibilityIdentifier("aiPrototype.send")
            }
        }
        .padding(.horizontal, 18)
        .padding(.vertical, 10)
    }

    private func sendDraft() {
        let prompt = draft.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !prompt.isEmpty else { return }
        store.sendAIPrototypePrompt(prompt)
        draft = ""
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
        case .ready: "Corpus ready · 4 sources"
        case .indexing: "Indexing local sources"
        case .offline: "Offline"
        case .cancelled: "Indexing cancelled"
        case .failed: "Indexing failed"
        default: state.rawValue.capitalized
        }
    }

    private func responseFailureLabel(_ state: PrototypeAIActivityState) -> String {
        switch state {
        case .rateLimited: "Provider rate limit reached; nothing was retried."
        case .offline: "Provider is offline; approved context remains local."
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

    private func proposalStateColor(_ state: PrototypeAIProposalState) -> Color {
        switch state {
        case .succeeded: .green
        case .failed: .orange
        case .running: .blue
        case .rejected, .cancelled: .secondary
        case .pending: .primary
        }
    }

    private func proposalIcon(_ kind: PrototypeAIProposalKind) -> String {
        switch kind {
        case .task: "slider.horizontal.3"
        case .python: "chevron.left.forwardslash.chevron.right"
        case .plot: "chart.xyaxis.line"
        case .download: "arrow.down.circle"
        case .note: "note.text"
        }
    }
}
