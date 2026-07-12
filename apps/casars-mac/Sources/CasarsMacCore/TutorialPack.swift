/// File-selection errors retained for the tutorial-template picker. Legacy
/// `tutorial-pack.v0` interpretation and GUI state live only in the Rust
/// one-shot migrator; Swift has no compatibility reader.
public enum TutorialPackLoadError: Error, Equatable, CustomStringConvertible {
    case missingManifest(String)

    public var description: String {
        switch self {
        case let .missingManifest(path):
            "No tutorial.md/tutorial.toml template or migratable pack.json found at \(path)"
        }
    }
}
