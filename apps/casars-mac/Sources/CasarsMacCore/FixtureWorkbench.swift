import Foundation

public enum FixtureWorkbench {
    public static func makeState() -> WorkbenchState {
        let datasets = [
            DatasetSummary(
                id: "ms-irc10216",
                name: "IRC+10216.ms",
                path: "project/data/IRC+10216.ms",
                kind: .measurementSet,
                size: "8.2 GB",
                units: "Jy, Hz, seconds",
                fields: ["IRC+10216", "J0954+1743", "3C286"],
                spectralWindows: ["spw 0: 1.41 GHz", "spw 1: 1.42 GHz", "spw 2: 1.43 GHz"],
                scans: ["1-4 target", "5-7 phase calibrator", "8 flux calibrator"],
                notes: "Fixture MeasurementSet with VLA-like spectral-line metadata."
            ),
            DatasetSummary(
                id: "image-cube",
                name: "IRC+10216.clean.image",
                path: "project/products/IRC+10216.clean.image",
                kind: .imageCube,
                size: "512 x 512 x 64",
                units: "Jy/beam",
                fields: ["IRC+10216"],
                spectralWindows: ["cube channels 0-63"],
                scans: [],
                notes: "Fixture image cube for movie and slice exploration."
            ),
            DatasetSummary(
                id: "phase-cal",
                name: "phase.cal",
                path: "project/calibration/phase.cal",
                kind: .calibrationTable,
                size: "24 MB",
                units: "complex gain",
                fields: ["J0954+1743"],
                spectralWindows: ["spw 0", "spw 1", "spw 2"],
                scans: ["5-7"],
                notes: "Fixture calibration table produced by a prior gain solve."
            ),
            DatasetSummary(
                id: "run-product",
                name: "calibrated.ms",
                path: "project/products/calibrated.ms",
                kind: .runProduct,
                size: "8.4 GB",
                units: "corrected visibilities",
                fields: ["IRC+10216"],
                spectralWindows: ["spw 0", "spw 1", "spw 2"],
                scans: ["1-4"],
                notes: "Fixture run output used by the history timeline."
            )
        ]

        let project = ProjectFixture(
            name: "VLA spectral-line demo",
            rootPath: "/FixtureProjects/vla-spectral-line-demo",
            datasets: datasets
        )

        return WorkbenchState(
            project: project,
            dockMode: .datasets,
            selectedDatasetID: "ms-irc10216",
            inspectorCollapsed: false,
            tabs: [
                WorkbenchTab(id: "tab-ms", title: "IRC+10216.ms", kind: .datasetExplorer, datasetID: "ms-irc10216"),
                WorkbenchTab(id: "tab-task", title: "Calibrate", kind: .task, datasetID: "ms-irc10216"),
                WorkbenchTab(id: "tab-ai", title: "AI Chat", kind: .aiChat),
                WorkbenchTab(id: "tab-python", title: "Python", kind: .python)
            ],
            activeTabID: "tab-ms",
            taskParameters: TaskParameters(
                taskName: "calibrate",
                selectedField: "IRC+10216",
                selectedSpectralWindow: "spw 1: 1.42 GHz",
                outputName: "calibrated.ms",
                dryRun: true
            ),
            taskRun: TaskRun(
                state: .idle,
                progress: 0.0,
                logLines: ["Fixture task is ready.", "No real data will be modified."],
                warnings: []
            ,
                products: []
            ),
            aiMessages: [
                AIChatMessage(
                    id: "msg-system",
                    author: .system,
                    text: "Fixture assistant has selected the active MeasurementSet as context."
                ),
                AIChatMessage(
                    id: "msg-assistant",
                    author: .assistant,
                    text: "The selected spectral windows cover the target line. I can narrow the task to spw 1 before the dry run."
                )
            ],
            aiProposals: [
                AIProposal(
                    id: "proposal-spw",
                    title: "Narrow calibration to line SPW",
                    detail: "Use the dataset-specific spectral-window option that covers the line center.",
                    parameterName: "Spectral window",
                    oldValue: "all",
                    newValue: "spw 1: 1.42 GHz",
                    state: .pending
                )
            ],
            python: PythonPanelState(
                owner: .user,
                buffer: """
                from casars import ms
                table = ms.open("project/data/IRC+10216.ms")
                table.plot_uvdist(spw="1")
                """,
                capturedPlots: ["Amplitude vs. channel preview", "UV distance scatter preview"]
            ),
            history: [
                ProcessingHistoryEvent(
                    id: "hist-import",
                    timestamp: "2026-05-04 09:12",
                    title: "Project opened",
                    reason: "User opened fixture project directory.",
                    affectedPaths: ["project/data/IRC+10216.ms"],
                    approval: "user"
                ),
                ProcessingHistoryEvent(
                    id: "hist-cal",
                    timestamp: "2026-05-04 09:18",
                    title: "Fixture calibration dry run",
                    reason: "AI proposed a narrower spectral window; user approval pending.",
                    affectedPaths: ["project/products/calibrated.ms"],
                    approval: "pending"
                )
            ],
            commandQuery: "",
            lastErrors: []
        )
    }
}
