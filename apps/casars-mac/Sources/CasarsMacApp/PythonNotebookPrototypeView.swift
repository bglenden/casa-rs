import CasarsMacCore
import SwiftUI

struct PythonNotebookPrototypeView: View {
    @Environment(\.colorScheme) private var colorScheme
    @ObservedObject var store: WorkbenchStore
    @State private var expandedExecutionHistory: Set<String> = []
    @State private var expandedVisualizationHistory: Set<String> = []

    private var projection: PrototypePythonNotebookProjection? {
        store.state.prototypePython
    }

    private var selectedCell: PrototypePythonCell? {
        projection?.selectedCell
    }

    var body: some View {
        VStack(spacing: 0) {
            if projection?.activeExplorer == nil { prototypeToolbar } else { explorerToolbar }
            Divider()
            prototypeDisclosure
            Divider()
            if projection?.activeExplorer == nil { continuousNotebookDocument } else { explorerSurface }
        }
        .sheet(isPresented: enlargedVisualizationBinding) { visualizationLightbox }
    }

    private var continuousNotebookDocument: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 18) {
                VStack(alignment: .leading, spacing: 7) {
                    Text("Calibrated visibility inspection")
                        .workbenchFont(.title2, weight: .semibold)
                    Text("Use Python to inspect the calibrated MeasurementSet, preserve ordered output, and keep every plot revision reproducible beside the surrounding scientific notes.")
                        .foregroundStyle(.secondary)
                }

                savedVisualizations

                ForEach(Array((projection?.cells ?? []).enumerated()), id: \.element.id) { index, cell in
                    if index == 1 {
                        notebookNote(
                            "The continuum amplitudes should decline smoothly with UV distance. Keep both vector and raster forms so the figure remains editable and portable."
                        )
                    } else if index == 3 {
                        notebookNote(
                            "The radial profile below is an AI proposal. Review the exact source before allowing it to run; editing the proposal must invalidate approval."
                        )
                    }
                    inlineCell(cell)
                }

                Label(
                    "\(projection?.insertedPlotCount ?? 0) plot revision(s) inserted into this notebook",
                    systemImage: "book.pages"
                )
                .workbenchFont(.caption, weight: .semibold)
                .foregroundStyle(.secondary)
                .padding(.bottom, 70)
                .accessibilityIdentifier("pythonPrototype.insertedPlotCount")
                .accessibilityValue("\(projection?.insertedPlotCount ?? 0)")
            }
            .padding(.horizontal, 44)
            .padding(.top, 28)
            .frame(maxWidth: 980, alignment: .leading)
            .frame(maxWidth: .infinity)
        }
        .background(Color(nsColor: .textBackgroundColor))
        .accessibilityIdentifier("pythonPrototype.documentScroll")
    }

    private var savedVisualizations: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack {
                Text("Saved figures").workbenchFont(.headline)
                Spacer()
                Text("\(projection?.savedVisualizations.count ?? 0) explicit snapshots · not live explorer views")
                    .workbenchFont(.caption)
                    .prototypeSecondaryForeground()
                    .accessibilityIdentifier("pythonPrototype.savedVisualizationCount")
                    .accessibilityValue("\(projection?.savedVisualizations.count ?? 0)")
            }
            LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 12) {
                ForEach(projection?.savedVisualizations ?? []) { visualization in
                    visualizationCard(visualization)
                }
            }
            .accessibilityElement(children: .contain)
            .accessibilityLabel("Saved notebook visualizations")
            .accessibilityIdentifier("notebookVisualization.collection")
        }
    }

    private func visualizationCard(_ visualization: PrototypeNotebookVisualization) -> some View {
        VStack(alignment: .leading, spacing: 8) {
            if let revision = visualization.latestRevision {
                Button {
                    store.setPrototypeEnlargedVisualization(visualization.id)
                } label: {
                    visualizationPreview(revision).frame(height: 145).contentShape(Rectangle())
                }
                .buttonStyle(.plain)
                .accessibilityIdentifier("notebookVisualization.preview.\(visualization.id)")
                .accessibilityLabel("Enlarge \(revision.title)")

                HStack(spacing: 8) {
                    VStack(alignment: .leading, spacing: 2) {
                        Text(revision.title).workbenchFont(.subheadline, weight: .semibold).lineLimit(1)
                        Text("Saved from \(revision.kind.sourceSurfaceTitle) · revision \(revision.sequence)")
                            .workbenchFont(.caption2)
                            .prototypeSecondaryForeground()
                            .accessibilityIdentifier("notebookVisualization.revisionCount.\(visualization.id)")
                            .accessibilityValue("\(visualization.revisions.count)")
                    }
                    Spacer()
                    Button("Open in Explorer") { store.openPrototypeExplorer(visualizationID: visualization.id) }
                        .accessibilityIdentifier("notebookVisualization.openExplorer.\(visualization.id)")
                }

                if visualization.revisions.count > 1 {
                    DisclosureGroup(
                        "Previous revisions (\(visualization.revisions.count - 1))",
                        isExpanded: historyBinding(visualization.id, in: $expandedVisualizationHistory)
                    ) {
                        ForEach(visualization.revisions.sorted(by: { $0.sequence > $1.sequence }).dropFirst()) { prior in
                            Text("Revision \(prior.sequence) · \(prior.assetPath)")
                                .workbenchFont(.caption2, design: .monospaced)
                                .prototypeSecondaryForeground()
                                .padding(.top, 4)
                        }
                    }
                    .workbenchFont(.caption, weight: .semibold)
                    .accessibilityIdentifier("notebookVisualization.previousRevisions.\(visualization.id)")
                }
            }
        }
        .padding(10)
        .background(Color.secondary.opacity(0.035))
        .overlay(RoundedRectangle(cornerRadius: 8).stroke(Color.secondary.opacity(0.16)))
        .clipShape(RoundedRectangle(cornerRadius: 8))
    }

    private func notebookNote(_ text: String) -> some View {
        Text(text)
            .frame(maxWidth: .infinity, alignment: .leading)
            .textSelection(.enabled)
    }

    private func inlineCell(_ cell: PrototypePythonCell) -> some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack(alignment: .top, spacing: 12) {
                Button {
                    store.selectPrototypePythonCell(cell.id)
                } label: {
                    HStack(alignment: .top, spacing: 9) {
                        Image(systemName: cell.owner == .ai ? "sparkles" : "chevron.left.forwardslash.chevron.right")
                            .foregroundStyle(cell.owner == .ai ? .purple : .secondary)
                        VStack(alignment: .leading, spacing: 3) {
                            Text(cell.title)
                                .workbenchFont(.headline)
                            Text(cell.owner == .ai ? "AI-proposed code · exact-source approval required" : "User code · normal user authority")
                                .workbenchFont(.caption)
                                .foregroundStyle(
                                    cell.owner == .ai
                                        ? Color.purple
                                        : Color(nsColor: .labelColor).opacity(0.82)
                                )
                                .accessibilityIdentifier(
                                    cell.id == projection?.selectedCellID
                                        ? "pythonPrototype.ownerDisclosure"
                                        : "pythonPrototype.ownerDisclosure.\(cell.id)"
                                )
                        }
                    }
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .contentShape(Rectangle())
                }
                .buttonStyle(.plain)
                .accessibilityIdentifier("pythonPrototype.cell.\(cell.id)")
                .accessibilityValue("\(cell.owner.rawValue), \(cell.latestRevision?.status.rawValue ?? "idle")")

                HStack(spacing: 7) {
                    statusDot(cell.latestRevision?.status ?? .idle)
                    Text(cellStatusLabel(cell))
                        .workbenchFont(.caption, weight: .semibold)
                        .prototypeSecondaryForeground()
                }

                if cell.id == projection?.selectedCellID {
                    Button {
                        store.runPrototypePythonCell(cell.id)
                    } label: {
                        Label(cell.latestRevision?.status == .failed ? "Retry" : "Run", systemImage: "play.fill")
                    }
                    .buttonStyle(.borderedProminent)
                    .disabled(projection?.kernelState != .ready || !cell.approvalIsValid)
                    .accessibilityIdentifier("pythonPrototype.run")
                }
            }

            if cell.id == projection?.selectedCellID {
                if cell.owner == .ai {
                    approvalCard(cell)
                }
                sourceEditor(cell)
                outputHistory(cell)
            } else {
                Text(cell.source.split(separator: "\n").prefix(3).joined(separator: "\n"))
                    .font(.system(size: 12, design: .monospaced))
                    .prototypeSecondaryForeground()
                    .lineLimit(3)
                    .padding(.horizontal, 11)
                    .padding(.vertical, 9)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .background(Color.secondary.opacity(0.045))
                    .clipShape(RoundedRectangle(cornerRadius: 6))
            }
        }
        .padding(13)
        .background(Color.secondary.opacity(cell.id == projection?.selectedCellID ? 0.07 : 0.035))
        .overlay(
            RoundedRectangle(cornerRadius: 9)
                .stroke(cell.id == projection?.selectedCellID ? Color.accentColor.opacity(0.32) : Color.secondary.opacity(0.15))
        )
        .clipShape(RoundedRectangle(cornerRadius: 9))
    }

    private var prototypeToolbar: some View {
        HStack(spacing: 12) {
            VStack(alignment: .leading, spacing: 2) {
                Text(projection?.notebookTitle ?? "Python notebook")
                    .workbenchFont(.title3, weight: .semibold)
                Text("Persistent per-notebook kernel · interaction prototype")
                    .workbenchFont(.caption)
                    .prototypeSecondaryForeground()
            }
            Spacer()
            kernelState
            Button {
                store.runAllPrototypePythonCells()
            } label: {
                Label("Run All", systemImage: "play.fill")
            }
            .disabled(projection?.kernelState != .ready)
            .accessibilityIdentifier("pythonPrototype.runAll")

            Button {
                store.interruptPrototypePythonKernel()
            } label: {
                Label("Stop", systemImage: "stop.fill")
            }
            .disabled(projection?.kernelState != .running)
            .accessibilityIdentifier("pythonPrototype.stop")

            Button {
                store.restartPrototypePythonKernel()
            } label: {
                Label("Restart", systemImage: "arrow.clockwise")
            }
            .disabled(projection?.kernelState == .ready)
            .accessibilityIdentifier("pythonPrototype.restart")
        }
        .padding(.horizontal, 18)
        .padding(.vertical, 11)
    }

    private var kernelState: some View {
        HStack(spacing: 6) {
            Circle()
                .fill(kernelColor)
                .frame(width: 7, height: 7)
            Text(kernelLabel)
                .workbenchFont(.caption, weight: .semibold)
        }
        .padding(.horizontal, 9)
        .padding(.vertical, 5)
        .background(kernelColor.opacity(0.12))
        .clipShape(Capsule())
        .accessibilityIdentifier("pythonPrototype.kernelState")
        .accessibilityValue(projection?.kernelState.rawValue ?? "unavailable")
    }

    private var prototypeDisclosure: some View {
        HStack(spacing: 10) {
            Image(systemName: "testtube.2")
                .foregroundStyle(.blue)
                .accessibilityHidden(true)
            Text("Prototype — deterministic fixtures only. No Python process, project file, network, task, or provider is used.")
                .workbenchFont(.caption, weight: .semibold)
            Spacer()
            Text("Boundary calls: \(store.prototypeProductionBoundaryInvocationCount)")
                .workbenchFont(.caption, weight: .semibold, design: .monospaced)
                .accessibilityIdentifier("pythonPrototype.boundaryAudit")
                .accessibilityValue("\(store.prototypeProductionBoundaryInvocationCount)")
        }
        .padding(.horizontal, 18)
        .padding(.vertical, 8)
        .background(Color.blue.opacity(0.08))
    }

    private func approvalCard(_ cell: PrototypePythonCell) -> some View {
        VStack(alignment: .leading, spacing: 9) {
            HStack {
                Label("Exact-code approval", systemImage: cell.approvalIsValid ? "checkmark.shield.fill" : "lock.shield")
                    .workbenchFont(.headline)
                Spacer()
                Text(cell.approvalIsValid ? "Approved" : "Approval required")
                    .workbenchFont(.caption, weight: .semibold)
                    .foregroundStyle(cell.approvalIsValid ? .green : .orange)
                    .accessibilityIdentifier("pythonPrototype.approvalState")
                    .accessibilityValue(cell.approvalIsValid ? "approved" : "required")
            }
            Text("Approval binds only to source hash \(cell.sourceDigest). Any edit invalidates it.")
                .workbenchFont(.caption, design: .monospaced)
                .prototypeSecondaryForeground()
            Button("Approve exact code") {
                store.approvePrototypePythonSource(cellID: cell.id)
            }
            .simultaneousGesture(TapGesture().onEnded {
                store.approvePrototypePythonSource(cellID: cell.id)
            })
            .disabled(cell.approvalIsValid)
            .accessibilityIdentifier("pythonPrototype.approve")
        }
        .padding(12)
        .background(Color.purple.opacity(0.08))
        .overlay(RoundedRectangle(cornerRadius: 8).stroke(Color.purple.opacity(0.28)))
        .clipShape(RoundedRectangle(cornerRadius: 8))
    }

    private func sourceEditor(_ cell: PrototypePythonCell) -> some View {
        VStack(alignment: .leading, spacing: 7) {
            HStack {
                Text("Code")
                    .workbenchFont(.headline)
                    .foregroundStyle(Color(nsColor: .labelColor))
                Spacer()
                Text(cell.sourceDigest)
                    .workbenchFont(.caption2, design: .monospaced)
                    .prototypeSecondaryForeground()
            }
            TextEditor(text: Binding(
                get: { selectedCell?.source ?? "" },
                set: { store.setPrototypePythonSource(cellID: cell.id, source: $0) }
            ))
            .font(.system(size: 13, design: .monospaced))
            .scrollContentBackground(.hidden)
            .padding(9)
            .frame(minHeight: 145)
            .background(Color(nsColor: .controlBackgroundColor))
            .overlay(RoundedRectangle(cornerRadius: 7).stroke(Color.secondary.opacity(0.28)))
            .clipShape(RoundedRectangle(cornerRadius: 7))
            .accessibilityIdentifier("pythonPrototype.editor")
        }
    }

    private func outputHistory(_ cell: PrototypePythonCell) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack {
                Text("Latest execution")
                    .workbenchFont(.headline)
                Spacer()
                Text("\(cell.revisions.count)")
                    .workbenchFont(.caption, weight: .semibold)
                    .accessibilityIdentifier("pythonPrototype.revisionCount")
                    .accessibilityValue("\(cell.revisions.count)")
            }
            if cell.revisions.isEmpty {
                Text("Not run yet. Code and outputs remain separate immutable revisions after execution.")
                    .prototypeSecondaryForeground()
                    .padding(.vertical, 8)
            } else if let latest = cell.latestRevision {
                revisionCard(cell: cell, revision: latest)
                let previous = cell.revisions.filter { $0.id != latest.id }.sorted { $0.sequence > $1.sequence }
                if !previous.isEmpty {
                    let historyIsExpanded = expandedExecutionHistory.contains(cell.id)
                    Button {
                        if historyIsExpanded {
                            expandedExecutionHistory.remove(cell.id)
                        } else {
                            expandedExecutionHistory.insert(cell.id)
                        }
                    } label: {
                        HStack(spacing: 5) {
                            Image(systemName: historyIsExpanded ? "chevron.down" : "chevron.right")
                                .imageScale(.small)
                            Text("Previous revisions (\(previous.count))")
                            Spacer()
                        }
                        .contentShape(Rectangle())
                    }
                    .buttonStyle(.plain)
                    .workbenchFont(.caption, weight: .semibold)
                    .accessibilityIdentifier("pythonPrototype.previousRevisions.\(cell.id)")
                    .accessibilityValue(historyIsExpanded ? "expanded" : "collapsed")
                    if historyIsExpanded {
                        VStack(alignment: .leading, spacing: 6) {
                            ForEach(previous) { compactRevisionRow($0) }
                        }
                        .padding(.top, 6)
                    }
                }
            }
        }
    }

    private func compactRevisionRow(_ revision: PrototypePythonExecutionRevision) -> some View {
        HStack(spacing: 7) {
            statusDot(revision.status)
            Text("Revision \(revision.sequence)")
                .workbenchFont(.caption, weight: .semibold)
            Text(revision.status.rawValue.capitalized)
                .workbenchFont(.caption)
                .prototypeSecondaryForeground()
            Spacer()
            if revision.plot != nil { Image(systemName: "chart.xyaxis.line").foregroundStyle(.secondary) }
            Text(revision.sourceDigest)
                .workbenchFont(.caption2, design: .monospaced)
                .prototypeSecondaryForeground()
        }
        .padding(.horizontal, 9)
        .padding(.vertical, 7)
        .background(Color.secondary.opacity(0.035))
        .clipShape(RoundedRectangle(cornerRadius: 6))
        .accessibilityElement(children: .ignore)
        .accessibilityLabel("Revision \(revision.sequence)")
        .accessibilityIdentifier("pythonPrototype.revision.\(revision.sequence)")
        .accessibilityValue(revision.status.rawValue)
    }

    private func revisionCard(cell: PrototypePythonCell, revision: PrototypePythonExecutionRevision) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack(spacing: 7) {
                statusDot(revision.status)
                Text("Revision \(revision.sequence) · \(revision.status.rawValue.capitalized)")
                    .workbenchFont(.subheadline, weight: .semibold)
                Spacer()
                Text(revision.sourceDigest)
                    .workbenchFont(.caption2, design: .monospaced)
                    .prototypeSecondaryForeground()
            }
            .accessibilityIdentifier("pythonPrototype.revision.\(revision.sequence)")
            .accessibilityValue(revision.status.rawValue)

            ForEach(revision.outputs.sorted(by: { $0.order < $1.order })) { output in
                Text("[\(output.channel.rawValue)]  \(output.text)")
                    .font(.system(size: 13, weight: .semibold, design: .monospaced))
                    .foregroundStyle(colorScheme == .dark ? Color.white : Color.black)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .padding(.horizontal, 7)
                    .padding(.vertical, 5)
                    .background(colorScheme == .dark ? Color.black : Color.white)
                    .clipShape(RoundedRectangle(cornerRadius: 5))
                    .accessibilityElement(children: .combine)
                    .accessibilityLabel("\(output.channel.rawValue): \(output.text)")
            }

            if let plot = revision.plot {
                plotCard(cellID: cell.id, plot: plot)
            }
        }
        .padding(12)
        .background(Color.secondary.opacity(0.055))
        .overlay(RoundedRectangle(cornerRadius: 8).stroke(Color.secondary.opacity(0.18)))
        .clipShape(RoundedRectangle(cornerRadius: 8))
    }

    private func plotCard(cellID: String, plot: PrototypePythonPlotRevision) -> some View {
        VStack(alignment: .leading, spacing: 9) {
            DeterministicScientificPlot(title: plot.title)
                .frame(height: 210)
                .accessibilityElement(children: .combine)
                .accessibilityLabel(plot.title)
                .accessibilityIdentifier("pythonPrototype.plot.\(plot.id)")
            HStack(spacing: 8) {
                artifactBadge("PNG", path: plot.pngPath, identifier: "pythonPrototype.artifact.png")
                artifactBadge("SVG", path: plot.svgPath, identifier: "pythonPrototype.artifact.svg")
                Spacer()
                Button("Regenerate") {
                    store.regeneratePrototypePythonPlot(cellID: cellID)
                }
                .disabled(projection?.kernelState != .ready)
                .accessibilityIdentifier("pythonPrototype.regenerate")
                Button(plot.insertedInNotebook ? "Inserted" : "Insert in notebook") {
                    store.insertPrototypePythonPlot(cellID: cellID, plotID: plot.id)
                }
                .disabled(plot.insertedInNotebook)
                .accessibilityIdentifier("pythonPrototype.insert")
                .accessibilityValue(plot.insertedInNotebook ? "inserted" : "not-inserted")
            }
        }
    }

    private func artifactBadge(_ label: String, path: String, identifier: String) -> some View {
        VStack(alignment: .leading, spacing: 2) {
            Text(label)
                .workbenchFont(.caption2, weight: .bold)
            Text(path)
                .workbenchFont(.caption2, design: .monospaced)
                .lineLimit(1)
                .prototypeSecondaryForeground()
        }
        .padding(.horizontal, 8)
        .padding(.vertical, 5)
        .background(Color.accentColor.opacity(0.08))
        .clipShape(RoundedRectangle(cornerRadius: 5))
        .accessibilityIdentifier(identifier)
        .accessibilityValue(path)
    }

    private var explorerToolbar: some View {
        HStack(spacing: 12) {
            Button { store.closePrototypeExplorer() } label: {
                Label("Back to Notebook", systemImage: "chevron.left")
            }
            .accessibilityIdentifier("explorerSnapshot.back")
            Divider().frame(height: 22)
            VStack(alignment: .leading, spacing: 2) {
                Text(explorerSurfaceTitle).workbenchFont(.title3, weight: .semibold)
                Text("Fixture explorer · restored from saved figure")
                    .workbenchFont(.caption)
                    .prototypeSecondaryForeground()
            }
            Spacer()
            Label("Save to Notebook", systemImage: "square.and.arrow.down")
                .workbenchFont(.caption, weight: .semibold)
                .accessibilityIdentifier("explorerSnapshot.save")
            Button("New plot") { store.saveNewPrototypeExplorerVisualization() }
                .accessibilityIdentifier("explorerSnapshot.saveNew")
            Button("Update") { store.updatePrototypeExplorerVisualization() }
                .disabled(projection?.activeExplorer?.targetVisualizationID == nil)
                .accessibilityIdentifier("explorerSnapshot.update")
        }
        .padding(.horizontal, 18)
        .padding(.vertical, 11)
    }

    private var explorerSurface: some View {
        HSplitView {
            ScrollView {
                VStack(alignment: .leading, spacing: 14) {
                    Text("View parameters").workbenchFont(.headline)
                    Text("These values were restored from the saved figure. Changes remain private to this explorer until you explicitly save a new plot or update the saved figure.")
                        .workbenchFont(.caption)
                        .prototypeSecondaryForeground()
                    VStack(alignment: .leading, spacing: 11) {
                        ForEach(projection?.activeExplorer?.parameters ?? []) { parameter in
                            VStack(alignment: .leading, spacing: 4) {
                                Text(parameter.label).workbenchFont(.caption, weight: .semibold)
                                TextField(parameter.label, text: Binding(
                                    get: {
                                        projection?.activeExplorer?.parameters
                                            .first(where: { $0.id == parameter.id })?.value ?? ""
                                    },
                                    set: { store.setPrototypeExplorerParameter(id: parameter.id, value: $0) }
                                ))
                                .accessibilityIdentifier("explorerSnapshot.parameter.\(parameter.id)")
                            }
                        }
                    }
                    .accessibilityElement(children: .contain)
                    .accessibilityIdentifier("explorerSnapshot.parameters")
                }
                .padding(18)
            }
            .frame(minWidth: 250, idealWidth: 300, maxWidth: 360)

            VStack(alignment: .leading, spacing: 12) {
                HStack {
                    VStack(alignment: .leading, spacing: 2) {
                        Text(projection?.activeExplorer?.title ?? "Explorer preview")
                            .workbenchFont(.title2, weight: .semibold)
                        Text("Unsaved explorer preview")
                            .workbenchFont(.caption, weight: .semibold)
                            .foregroundStyle(.orange)
                    }
                    Spacer()
                    Label("Notebook unchanged", systemImage: "lock.doc")
                        .workbenchFont(.caption, weight: .semibold)
                        .prototypeSecondaryForeground()
                        .accessibilityIdentifier("explorerSnapshot.targetRevisionCount")
                        .accessibilityValue("\(activeExplorerTargetRevisionCount)")
                }
                if let session = projection?.activeExplorer {
                    visualizationPreview(PrototypeNotebookVisualizationRevision(
                        id: "active-explorer-preview",
                        sequence: 0,
                        title: session.title,
                        kind: session.kind,
                        parameters: session.parameters,
                        assetPath: "fixture://unsaved"
                    ))
                    .accessibilityIdentifier("explorerSnapshot.preview")
                }
                Text("Edit the controls to generate a new preview. Nothing in the notebook changes until Save to Notebook is used.")
                    .prototypeSecondaryForeground()
            }
            .padding(24)
            .frame(minWidth: 520, maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            .background(Color(nsColor: .textBackgroundColor))
        }
    }

    private var explorerSurfaceTitle: String {
        projection?.activeExplorer?.kind == .imageView ? "Image Explorer" : "MeasurementSet Explorer"
    }

    private var activeExplorerTargetRevisionCount: Int {
        guard let targetID = projection?.activeExplorer?.targetVisualizationID else { return 0 }
        return projection?.savedVisualizations.first { $0.id == targetID }?.revisions.count ?? 0
    }

    @ViewBuilder
    private func visualizationPreview(_ revision: PrototypeNotebookVisualizationRevision) -> some View {
        switch revision.kind {
        case .measurementSetPlot: DeterministicScientificPlot(title: revision.title)
        case .imageView: DeterministicScientificImage(title: revision.title)
        }
    }

    private var enlargedVisualizationBinding: Binding<Bool> {
        Binding(
            get: { projection?.enlargedVisualizationID != nil },
            set: { if !$0 { store.setPrototypeEnlargedVisualization(nil) } }
        )
    }

    @ViewBuilder
    private var visualizationLightbox: some View {
        if let visualization = projection?.enlargedVisualization,
           let revision = visualization.latestRevision
        {
            VStack(alignment: .leading, spacing: 14) {
                HStack {
                    VStack(alignment: .leading, spacing: 2) {
                        Text(revision.title).workbenchFont(.title2, weight: .semibold)
                        Text("Saved snapshot · revision \(revision.sequence) · \(revision.assetPath)")
                            .workbenchFont(.caption, design: .monospaced)
                            .prototypeSecondaryForeground()
                    }
                    Spacer()
                    Button("Open in Explorer") {
                        store.setPrototypeEnlargedVisualization(nil)
                        store.openPrototypeExplorer(visualizationID: visualization.id)
                    }
                    .accessibilityIdentifier("notebookVisualization.lightboxOpenExplorer")
                    Button("Done") { store.setPrototypeEnlargedVisualization(nil) }
                        .accessibilityIdentifier("notebookVisualization.lightboxDone")
                }
                visualizationPreview(revision)
                    .frame(minWidth: 760, minHeight: 480)
                    .accessibilityIdentifier("notebookVisualization.lightbox.\(visualization.id)")
            }
            .padding(24)
        }
    }

    private func historyBinding(_ id: String, in values: Binding<Set<String>>) -> Binding<Bool> {
        Binding(
            get: { values.wrappedValue.contains(id) },
            set: { isExpanded in
                if isExpanded { values.wrappedValue.insert(id) } else { values.wrappedValue.remove(id) }
            }
        )
    }

    private func statusDot(_ status: PrototypePythonCellStatus) -> some View {
        Circle()
            .fill(statusColor(status))
            .frame(width: 7, height: 7)
    }

    private func cellStatusLabel(_ cell: PrototypePythonCell) -> String {
        guard let revision = cell.latestRevision else {
            return cell.owner == .ai && !cell.approvalIsValid ? "Approval required" : "Not run"
        }
        return "r\(revision.sequence) · \(revision.status.rawValue)"
    }

    private var kernelLabel: String {
        switch projection?.kernelState {
        case .ready: "Kernel ready"
        case .running: "Running"
        case .restartRequired: "Restart required"
        case nil: "Unavailable"
        }
    }

    private var kernelColor: Color {
        switch projection?.kernelState {
        case .ready: .green
        case .running: .blue
        case .restartRequired: .orange
        case nil: .secondary
        }
    }

    private func statusColor(_ status: PrototypePythonCellStatus) -> Color {
        switch status {
        case .idle: .secondary
        case .running: .blue
        case .succeeded: .green
        case .failed: .red
        case .interrupted: .orange
        }
    }

}

private struct DeterministicScientificPlot: View {
    let title: String

    private let points: [(Double, Double)] = [
        (0.04, 0.87), (0.09, 0.78), (0.14, 0.81), (0.19, 0.69),
        (0.24, 0.73), (0.29, 0.60), (0.34, 0.63), (0.39, 0.51),
        (0.44, 0.55), (0.49, 0.43), (0.54, 0.47), (0.59, 0.36),
        (0.64, 0.40), (0.69, 0.29), (0.74, 0.32), (0.79, 0.23),
        (0.84, 0.26), (0.89, 0.18), (0.94, 0.20),
    ]

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            Text(title)
                .workbenchFont(.subheadline, weight: .semibold)
                .foregroundStyle(Color(nsColor: .labelColor))
            Canvas { context, size in
                let inset: CGFloat = 28
                let frame = CGRect(
                    x: inset,
                    y: 8,
                    width: max(1, size.width - inset - 8),
                    height: max(1, size.height - inset - 8)
                )
                var axes = Path()
                axes.move(to: CGPoint(x: frame.minX, y: frame.minY))
                axes.addLine(to: CGPoint(x: frame.minX, y: frame.maxY))
                axes.addLine(to: CGPoint(x: frame.maxX, y: frame.maxY))
                context.stroke(axes, with: .color(.secondary.opacity(0.65)), lineWidth: 1)

                for (x, y) in points {
                    let center = CGPoint(
                        x: frame.minX + frame.width * x,
                        y: frame.maxY - frame.height * y
                    )
                    let dot = Path(ellipseIn: CGRect(x: center.x - 2.3, y: center.y - 2.3, width: 4.6, height: 4.6))
                    context.fill(dot, with: .color(.cyan.opacity(0.78)))
                }

                var trend = Path()
                trend.move(to: CGPoint(x: frame.minX, y: frame.minY + frame.height * 0.12))
                trend.addCurve(
                    to: CGPoint(x: frame.maxX, y: frame.minY + frame.height * 0.82),
                    control1: CGPoint(x: frame.minX + frame.width * 0.28, y: frame.minY + frame.height * 0.18),
                    control2: CGPoint(x: frame.minX + frame.width * 0.65, y: frame.minY + frame.height * 0.72)
                )
                context.stroke(trend, with: .color(.blue), lineWidth: 2)
            }
            .background(Color.black.opacity(0.16))
            .clipShape(RoundedRectangle(cornerRadius: 6))
            HStack {
                Text("UV distance (kλ)")
                Spacer()
                Text("Amplitude (Jy)")
            }
            .workbenchFont(.caption2)
            .prototypeSecondaryForeground()
        }
        .padding(10)
        .background(Color(nsColor: .controlBackgroundColor))
        .overlay(RoundedRectangle(cornerRadius: 8).stroke(Color.secondary.opacity(0.24)))
        .clipShape(RoundedRectangle(cornerRadius: 8))
    }
}

private struct DeterministicScientificImage: View {
    let title: String

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            Text(title)
                .workbenchFont(.subheadline, weight: .semibold)
                .foregroundStyle(Color(nsColor: .labelColor))
            Canvas { context, size in
                let center = CGPoint(x: size.width * 0.5, y: size.height * 0.52)
                let maximum = min(size.width, size.height) * 0.42
                for index in stride(from: 18, through: 1, by: -1) {
                    let fraction = CGFloat(index) / 18
                    let radius = maximum * fraction
                    let ellipse = Path(ellipseIn: CGRect(
                        x: center.x - radius * 1.35,
                        y: center.y - radius * 0.72,
                        width: radius * 2.7,
                        height: radius * 1.44
                    ))
                    context.fill(ellipse, with: .color(.orange.opacity(0.05 + Double(1 - fraction) * 0.12)))
                }
                let beam = Path(ellipseIn: CGRect(x: 15, y: size.height - 28, width: 18, height: 10))
                context.stroke(beam, with: .color(.white.opacity(0.8)), lineWidth: 1)
            }
            .background(LinearGradient(
                colors: [.black, Color(red: 0.12, green: 0.02, blue: 0.18)],
                startPoint: .topLeading,
                endPoint: .bottomTrailing
            ))
            .clipShape(RoundedRectangle(cornerRadius: 6))
            HStack { Text("Right ascension"); Spacer(); Text("Declination") }
                .workbenchFont(.caption2)
                .prototypeSecondaryForeground()
        }
        .padding(10)
        .background(Color(nsColor: .controlBackgroundColor))
        .overlay(RoundedRectangle(cornerRadius: 8).stroke(Color.secondary.opacity(0.24)))
        .clipShape(RoundedRectangle(cornerRadius: 8))
    }
}

private extension View {
    /// Keeps secondary prototype copy quiet while meeting normal-text contrast
    /// on both macOS light and dark control backgrounds.
    func prototypeSecondaryForeground() -> some View {
        foregroundStyle(Color(nsColor: .labelColor).opacity(0.82))
    }
}
