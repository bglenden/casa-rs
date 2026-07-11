import CasarsMacCore
import SwiftUI

struct PythonNotebookPrototypeView: View {
    @Environment(\.colorScheme) private var colorScheme
    @ObservedObject var store: WorkbenchStore

    private var projection: PrototypePythonNotebookProjection? {
        store.state.prototypePython
    }

    private var selectedCell: PrototypePythonCell? {
        projection?.selectedCell
    }

    var body: some View {
        VStack(spacing: 0) {
            prototypeToolbar
            Divider()
            prototypeDisclosure
            Divider()
            continuousNotebookDocument
        }
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
                                .foregroundStyle(cell.owner == .ai ? .purple : .secondary)
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
                        .foregroundStyle(.secondary)
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
                    .foregroundStyle(.secondary)
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
                    .foregroundStyle(.secondary)
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
                .foregroundStyle(.secondary)
            Button("Approve exact code") {
                store.approvePrototypePythonSource(cellID: cell.id)
            }
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
                Spacer()
                Text(cell.sourceDigest)
                    .workbenchFont(.caption2, design: .monospaced)
                    .foregroundStyle(.secondary)
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
                Text("Execution revisions")
                    .workbenchFont(.headline)
                Spacer()
                Text("\(cell.revisions.count)")
                    .workbenchFont(.caption, weight: .semibold)
                    .accessibilityIdentifier("pythonPrototype.revisionCount")
                    .accessibilityValue("\(cell.revisions.count)")
            }
            if cell.revisions.isEmpty {
                Text("Not run yet. Code and outputs remain separate immutable revisions after execution.")
                    .foregroundStyle(.secondary)
                    .padding(.vertical, 8)
            } else {
                ForEach(Array(cell.revisions.sorted(by: { $0.sequence > $1.sequence }).prefix(3))) { revision in
                    revisionCard(cell: cell, revision: revision)
                }
            }
        }
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
                    .foregroundStyle(.secondary)
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
                .foregroundStyle(.secondary)
        }
        .padding(.horizontal, 8)
        .padding(.vertical, 5)
        .background(Color.accentColor.opacity(0.08))
        .clipShape(RoundedRectangle(cornerRadius: 5))
        .accessibilityIdentifier(identifier)
        .accessibilityValue(path)
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
            .foregroundStyle(.secondary)
        }
        .padding(10)
        .background(Color(nsColor: .controlBackgroundColor))
        .overlay(RoundedRectangle(cornerRadius: 8).stroke(Color.secondary.opacity(0.24)))
        .clipShape(RoundedRectangle(cornerRadius: 8))
    }
}
