// swift-tools-version: 5.9

import PackageDescription

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
                    "-L", "../../target/debug",
                    "-L", "../../target/release",
                    "-lcasars_frontend_services",
                    "-Xlinker", "-rpath",
                    "-Xlinker", "../../target/release",
                    "-Xlinker", "-rpath",
                    "-Xlinker", "../../target/debug"
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
