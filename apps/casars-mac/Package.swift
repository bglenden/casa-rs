// swift-tools-version: 5.9

import PackageDescription
import Foundation

let packageRoot = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
let repositoryRoot = packageRoot.deletingLastPathComponent().deletingLastPathComponent()
let configuredCargoTarget = ProcessInfo.processInfo.environment["CARGO_TARGET_DIR"]
    .map { URL(fileURLWithPath: $0, relativeTo: repositoryRoot).standardizedFileURL }
    ?? repositoryRoot.appendingPathComponent("target", isDirectory: true)
let cargoDebugDirectory = configuredCargoTarget.appendingPathComponent("debug", isDirectory: true).path
let cargoReleaseDirectory = configuredCargoTarget.appendingPathComponent("release", isDirectory: true).path

let package = Package(
    name: "casars-mac",
    platforms: [
        .macOS(.v13)
    ],
    products: [
        .library(
            name: "CasarsMacCore",
            targets: ["CasarsMacCore"]
        ),
        .executable(
            name: "casars-mac",
            targets: ["CasarsMacApp"]
        )
    ],
    targets: [
        .target(
            name: "CasarsFrontendServices",
            dependencies: ["CasarsFrontendServicesFFI"],
            linkerSettings: [
                .unsafeFlags([
                    "-L", cargoDebugDirectory,
                    "-L", cargoReleaseDirectory,
                    "-lcasars_frontend_services",
                    "-Xlinker", "-rpath",
                    "-Xlinker", cargoReleaseDirectory,
                    "-Xlinker", "-rpath",
                    "-Xlinker", cargoDebugDirectory
                ])
            ]
        ),
        .systemLibrary(
            name: "CasarsFrontendServicesFFI"
        ),
        .target(
            name: "CasarsMacCore",
            dependencies: ["CasarsFrontendServices"],
            resources: [
                .copy("Resources/assistant-corpus")
            ],
            linkerSettings: [
                .linkedFramework("PDFKit"),
                .linkedFramework("Vision")
            ]
        ),
        .executableTarget(
            name: "CasarsMacApp",
            dependencies: ["CasarsMacCore"]
        ),
        .testTarget(
            name: "CasarsMacCoreTests",
            dependencies: ["CasarsMacCore"]
        )
    ]
)
