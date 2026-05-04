public enum EmptyWorkbench {
    public static func makeState(interfaceFontSize: Double = WorkbenchState.defaultInterfaceFontSize) -> WorkbenchState {
        WorkbenchState(
            project: ProjectFixture(
                name: "No Project",
                rootPath: "Open a project directory to begin",
                datasets: [],
                source: .none
            ),
            dockMode: .datasets,
            leftDockCollapsed: false,
            selectedDatasetID: nil,
            inspectorCollapsed: false,
            tabs: [],
            activeTabID: "",
            taskParameters: TaskParameters(
                taskName: "Calibrate",
                selectedField: "",
                selectedSpectralWindow: "all",
                outputName: "",
                dryRun: true
            ),
            taskRun: TaskRun(
                state: .idle,
                progress: 0,
                logLines: [],
                warnings: [],
                products: []
            ),
            aiMessages: [],
            aiProposals: [],
            python: PythonPanelState(owner: .user, buffer: "", capturedPlots: []),
            history: [],
            commandQuery: "",
            lastErrors: [],
            interfaceFontSize: interfaceFontSize
        )
    }
}
