import Foundation

/// Fixture-only scenarios for the Wave 2 Python notebook interaction review.
/// None of these values are persisted or exposed as a Python/runtime contract.
package enum PythonPrototypeScenario: String, Codable, Equatable {
    case primary = "happy-path"
    case failure
    case nonresponsive
}

package enum PrototypePythonKernelState: String, Codable, Equatable {
    case ready
    case running
    case restartRequired = "restart-required"
}

package enum PrototypePythonCellStatus: String, Codable, Equatable {
    case idle
    case running
    case succeeded
    case failed
    case interrupted
}

package enum PrototypePythonOutputChannel: String, Codable, Equatable {
    case stdout
    case stderr
    case error
}

package enum PrototypePythonCellBehavior: String, Codable, Equatable {
    case standard
    case plot
    case failure
    case nonresponsive
}

/// Fixture-only presentation intent supplied by the visualization producer.
/// The view never guesses from axis labels, units, or pixels at runtime.
package enum PrototypeVisualizationAspect: String, Codable, Equatable {
    case standardFourThree = "4:3"
    case squareData = "square-data"
}

package struct PrototypePythonOutputEvent: Identifiable, Codable, Equatable {
    package let id: String
    package var order: Int
    package var channel: PrototypePythonOutputChannel
    package var text: String

    package init(id: String, order: Int, channel: PrototypePythonOutputChannel, text: String) {
        self.id = id
        self.order = order
        self.channel = channel
        self.text = text
    }
}

package struct PrototypePythonPlotRevision: Identifiable, Codable, Equatable {
    package let id: String
    package var sequence: Int
    package var title: String
    package var pngPath: String
    package var svgPath: String
    package var presentationAspect: PrototypeVisualizationAspect
    package var insertedInNotebook: Bool

    package init(
        id: String,
        sequence: Int,
        title: String,
        pngPath: String,
        svgPath: String,
        presentationAspect: PrototypeVisualizationAspect,
        insertedInNotebook: Bool = false
    ) {
        self.id = id
        self.sequence = sequence
        self.title = title
        self.pngPath = pngPath
        self.svgPath = svgPath
        self.presentationAspect = presentationAspect
        self.insertedInNotebook = insertedInNotebook
    }
}

package struct PrototypePythonExecutionRevision: Identifiable, Codable, Equatable {
    package let id: String
    package var sequence: Int
    package var status: PrototypePythonCellStatus
    package var sourceDigest: String
    package var outputs: [PrototypePythonOutputEvent]
    package var plot: PrototypePythonPlotRevision?

    package init(
        id: String,
        sequence: Int,
        status: PrototypePythonCellStatus,
        sourceDigest: String,
        outputs: [PrototypePythonOutputEvent] = [],
        plot: PrototypePythonPlotRevision? = nil
    ) {
        self.id = id
        self.sequence = sequence
        self.status = status
        self.sourceDigest = sourceDigest
        self.outputs = outputs
        self.plot = plot
    }
}

package struct PrototypePythonCell: Identifiable, Codable, Equatable {
    package let id: String
    package var title: String
    package var source: String
    package var owner: PythonOwner
    package var behavior: PrototypePythonCellBehavior
    package var approvedSourceDigest: String?
    package var revisions: [PrototypePythonExecutionRevision]

    package init(
        id: String,
        title: String,
        source: String,
        owner: PythonOwner,
        behavior: PrototypePythonCellBehavior,
        approvedSourceDigest: String? = nil,
        revisions: [PrototypePythonExecutionRevision] = []
    ) {
        self.id = id
        self.title = title
        self.source = source
        self.owner = owner
        self.behavior = behavior
        self.approvedSourceDigest = approvedSourceDigest
        self.revisions = revisions
    }

    package var sourceDigest: String { PrototypePythonSourceDigest.make(source) }
    package var approvalIsValid: Bool {
        owner == .user || approvedSourceDigest == sourceDigest
    }
    package var latestRevision: PrototypePythonExecutionRevision? {
        revisions.max { $0.sequence < $1.sequence }
    }
}

package enum PrototypeVisualizationKind: String, Codable, Equatable {
    case measurementSetPlot = "measurement-set-plot"
    case imageView = "image-view"

    package var sourceSurfaceTitle: String {
        switch self {
        case .measurementSetPlot: "MeasurementSet Explorer"
        case .imageView: "Image Explorer"
        }
    }
}

package struct PrototypeExplorerParameter: Identifiable, Codable, Equatable {
    package let id: String
    package var label: String
    package var value: String
}

package struct PrototypeNotebookVisualizationRevision: Identifiable, Codable, Equatable {
    package let id: String
    package var sequence: Int
    package var title: String
    package var kind: PrototypeVisualizationKind
    package var parameters: [PrototypeExplorerParameter]
    package var assetPath: String
    package var presentationAspect: PrototypeVisualizationAspect

    package init(
        id: String,
        sequence: Int,
        title: String,
        kind: PrototypeVisualizationKind,
        parameters: [PrototypeExplorerParameter],
        assetPath: String,
        presentationAspect: PrototypeVisualizationAspect
    ) {
        self.id = id
        self.sequence = sequence
        self.title = title
        self.kind = kind
        self.parameters = parameters
        self.assetPath = assetPath
        self.presentationAspect = presentationAspect
    }
}

package struct PrototypeNotebookVisualization: Identifiable, Codable, Equatable {
    package let id: String
    package var revisions: [PrototypeNotebookVisualizationRevision]

    package var latestRevision: PrototypeNotebookVisualizationRevision? {
        revisions.max { $0.sequence < $1.sequence }
    }
}

package struct PrototypeExplorerSession: Codable, Equatable {
    package var kind: PrototypeVisualizationKind
    package var title: String
    package var parameters: [PrototypeExplorerParameter]
    package var presentationAspect: PrototypeVisualizationAspect
    package var targetVisualizationID: String?
}

/// Mutable, in-memory-only Wave 2 interaction projection.
package struct PrototypePythonNotebookProjection: Codable, Equatable {
    package var prototypeKind: WorkbenchPrototypeKind
    package var scenario: PythonPrototypeScenario
    package var notebookTitle: String
    package var cells: [PrototypePythonCell]
    package var selectedCellID: String
    package var kernelState: PrototypePythonKernelState
    package var runningCellID: String?
    package var nextExecutionSequence: Int
    package var savedVisualizations: [PrototypeNotebookVisualization]
    package var activeExplorer: PrototypeExplorerSession?
    package var enlargedVisualizationID: String?
    package var nextVisualizationSequence: Int

    package init(
        prototypeKind: WorkbenchPrototypeKind = .python,
        scenario: PythonPrototypeScenario,
        notebookTitle: String,
        cells: [PrototypePythonCell],
        selectedCellID: String,
        kernelState: PrototypePythonKernelState = .ready,
        runningCellID: String? = nil,
        nextExecutionSequence: Int = 1,
        savedVisualizations: [PrototypeNotebookVisualization] = [],
        activeExplorer: PrototypeExplorerSession? = nil,
        enlargedVisualizationID: String? = nil,
        nextVisualizationSequence: Int = 1
    ) {
        self.prototypeKind = prototypeKind
        self.scenario = scenario
        self.notebookTitle = notebookTitle
        self.cells = cells
        self.selectedCellID = selectedCellID
        self.kernelState = kernelState
        self.runningCellID = runningCellID
        self.nextExecutionSequence = nextExecutionSequence
        self.savedVisualizations = savedVisualizations
        self.activeExplorer = activeExplorer
        self.enlargedVisualizationID = enlargedVisualizationID
        self.nextVisualizationSequence = nextVisualizationSequence
    }

    package var selectedCell: PrototypePythonCell? {
        cells.first { $0.id == selectedCellID }
    }

    package var insertedPlotCount: Int {
        cells.flatMap(\.revisions).compactMap(\.plot).filter(\.insertedInNotebook).count
    }

    package var enlargedVisualization: PrototypeNotebookVisualization? {
        savedVisualizations.first { $0.id == enlargedVisualizationID }
    }
}

package enum PrototypePythonFixtureAdapter {
    package static func make(scenario: PythonPrototypeScenario) -> PrototypePythonNotebookProjection {
        var cells = [
            PrototypePythonCell(
                id: "python-cell-summary",
                title: "Inspect the calibrated MeasurementSet",
                source: """
                from casars import msexplore

                summary = msexplore.summary("data/twhya_calibrated.ms")
                print(f"{summary.rows:,} rows · {summary.spws} spectral windows")
                """,
                owner: .user,
                behavior: .standard
            ),
            PrototypePythonCell(
                id: "python-cell-plot",
                title: "Amplitude versus UV distance",
                source: """
                data = msexplore.data(
                    "data/twhya_calibrated.ms",
                    x="uvdist", y="amplitude", field="TW Hya"
                )
                fig, ax = data.plot(marker=".", alpha=0.35)
                ax.set_title("TW Hya · calibrated visibilities")
                """,
                owner: .user,
                behavior: .plot
            ),
            PrototypePythonCell(
                id: "python-cell-repair",
                title: "Failure and retry",
                source: """
                print("checking continuum selection", flush=True)
                raise RuntimeError("fixture: channel selection is empty")
                """,
                owner: .user,
                behavior: .failure
            ),
            PrototypePythonCell(
                id: "python-cell-ai",
                title: "AI-proposed radial profile",
                source: """
                image = casars.images.open("products/twhya.image")
                profile = image.radial_profile(center="peak", bins=48)
                profile.plot(label="AI proposal")
                """,
                owner: .ai,
                behavior: .plot
            ),
            PrototypePythonCell(
                id: "python-cell-nonresponsive",
                title: "Interrupt and forced restart",
                source: """
                # Fixture deliberately ignores the first interrupt.
                while True:
                    pass
                """,
                owner: .user,
                behavior: .nonresponsive
            ),
        ]
        cells[1].revisions = [completedPlotRevision(
            sequence: 1,
            sourceDigest: cells[1].sourceDigest
        )]

        let selectedCellID: String
        let kernelState: PrototypePythonKernelState
        switch scenario {
        case .primary:
            selectedCellID = "python-cell-plot"
            kernelState = .ready
        case .failure:
            selectedCellID = "python-cell-repair"
            kernelState = .ready
            cells[2].revisions = [failedRevision(
                sequence: 2,
                sourceDigest: cells[2].sourceDigest
            )]
        case .nonresponsive:
            selectedCellID = "python-cell-nonresponsive"
            kernelState = .running
            cells[4].revisions = [runningRevision(
                sequence: 2,
                sourceDigest: cells[4].sourceDigest
            )]
        }

        return PrototypePythonNotebookProjection(
            scenario: scenario,
            notebookTitle: "TW Hya analysis",
            cells: cells,
            selectedCellID: selectedCellID,
            kernelState: kernelState,
            runningCellID: scenario == .nonresponsive ? "python-cell-nonresponsive" : nil,
            nextExecutionSequence: 3,
            savedVisualizations: savedVisualizationFixtures(),
            nextVisualizationSequence: 3
        )
    }

    private static func savedVisualizationFixtures() -> [PrototypeNotebookVisualization] {
        [
            PrototypeNotebookVisualization(
                id: "saved-visibility-plot",
                revisions: [PrototypeNotebookVisualizationRevision(
                    id: "saved-visibility-plot-r1",
                    sequence: 1,
                    title: "TW Hya · amplitude vs UV distance",
                    kind: .measurementSetPlot,
                    parameters: [
                        PrototypeExplorerParameter(id: "x-axis", label: "X axis", value: "UV distance"),
                        PrototypeExplorerParameter(id: "y-axis", label: "Y axis", value: "Amplitude"),
                        PrototypeExplorerParameter(id: "field", label: "Field", value: "TW Hya"),
                        PrototypeExplorerParameter(id: "averaging", label: "Channel averaging", value: "8"),
                    ],
                    assetPath: "notebooks/assets/explorers/saved-visibility-plot/r1.png",
                    presentationAspect: .standardFourThree
                )]
            ),
            PrototypeNotebookVisualization(
                id: "saved-continuum-image",
                revisions: [PrototypeNotebookVisualizationRevision(
                    id: "saved-continuum-image-r1",
                    sequence: 1,
                    title: "TW Hya · continuum image",
                    kind: .imageView,
                    parameters: [
                        PrototypeExplorerParameter(id: "plane", label: "Plane", value: "0"),
                        PrototypeExplorerParameter(id: "stretch", label: "Stretch", value: "Asinh"),
                        PrototypeExplorerParameter(id: "color-map", label: "Color map", value: "Inferno"),
                    ],
                    assetPath: "notebooks/assets/explorers/saved-continuum-image/r1.png",
                    presentationAspect: .squareData
                )]
            ),
        ]
    }

    private static func completedPlotRevision(
        sequence: Int,
        sourceDigest: String
    ) -> PrototypePythonExecutionRevision {
        PrototypePythonExecutionRevision(
            id: "python-execution-\(sequence)",
            sequence: sequence,
            status: .succeeded,
            sourceDigest: sourceDigest,
            outputs: [
                PrototypePythonOutputEvent(
                    id: "python-output-\(sequence)-1",
                    order: 1,
                    channel: .stdout,
                    text: "Loaded 12,480 averaged visibility samples."
                ),
                PrototypePythonOutputEvent(
                    id: "python-output-\(sequence)-2",
                    order: 2,
                    channel: .stderr,
                    text: "Matplotlib: using deterministic fixture renderer."
                ),
            ],
            plot: PrototypePythonPlotRevision(
                id: "python-plot-\(sequence)",
                sequence: sequence,
                title: "TW Hya · amplitude vs UV distance",
                pngPath: "notebooks/assets/python-cell-plot/execution-\(sequence)/figure-1.png",
                svgPath: "notebooks/assets/python-cell-plot/execution-\(sequence)/figure-1.svg",
                presentationAspect: .standardFourThree
            )
        )
    }

    private static func failedRevision(
        sequence: Int,
        sourceDigest: String
    ) -> PrototypePythonExecutionRevision {
        PrototypePythonExecutionRevision(
            id: "python-execution-\(sequence)",
            sequence: sequence,
            status: .failed,
            sourceDigest: sourceDigest,
            outputs: [
                PrototypePythonOutputEvent(
                    id: "python-output-\(sequence)-1",
                    order: 1,
                    channel: .stdout,
                    text: "checking continuum selection"
                ),
                PrototypePythonOutputEvent(
                    id: "python-output-\(sequence)-2",
                    order: 2,
                    channel: .error,
                    text: "RuntimeError: fixture: channel selection is empty"
                ),
            ]
        )
    }

    private static func runningRevision(
        sequence: Int,
        sourceDigest: String
    ) -> PrototypePythonExecutionRevision {
        PrototypePythonExecutionRevision(
            id: "python-execution-\(sequence)",
            sequence: sequence,
            status: .running,
            sourceDigest: sourceDigest,
            outputs: [
                PrototypePythonOutputEvent(
                    id: "python-output-\(sequence)-1",
                    order: 1,
                    channel: .stdout,
                    text: "Entered deterministic nonresponsive fixture."
                )
            ]
        )
    }
}

package enum PrototypePythonSourceDigest {
    package static func make(_ source: String) -> String {
        var hash: UInt64 = 14_695_981_039_346_656_037
        for byte in source.utf8 {
            hash ^= UInt64(byte)
            hash &*= 1_099_511_628_211
        }
        return String(format: "%016llx", hash)
    }
}
