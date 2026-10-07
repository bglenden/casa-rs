import AppKit
import Foundation
import SwiftUI

struct FirstRunOnboardingView: View {
    let errorMessage: String?
    let startTutorial: () -> Void
    let openProject: () -> Void
    let openDemo: () -> Void
    let dismiss: () -> Void

    var body: some View {
        ZStack {
            Color.black.opacity(0.42)

            VStack(alignment: .leading, spacing: 18) {
                HStack(alignment: .top, spacing: 16) {
                    Image(systemName: "sparkles.rectangle.stack.fill")
                        .font(.system(size: 34))
                        .foregroundStyle(.blue)
                        .frame(width: 54, height: 54)
                        .background(Color.blue.opacity(0.12))
                        .clipShape(RoundedRectangle(cornerRadius: 14))

                    VStack(alignment: .leading, spacing: 5) {
                        Text("Welcome to casa-rs")
                            .font(.system(size: 25, weight: .semibold))
                            .accessibilityIdentifier("onboarding.welcome")
                        Text("Explore radio astronomy data in a workspace that keeps your data, notes, plots, and CASA-style tasks together.")
                            .foregroundStyle(.secondary)
                            .fixedSize(horizontal: false, vertical: true)
                    }

                    Spacer(minLength: 8)

                    Button(action: dismiss) {
                        Image(systemName: "xmark")
                            .font(.caption.weight(.semibold))
                            .frame(width: 22, height: 22)
                    }
                    .buttonStyle(.borderless)
                    .help("Dismiss welcome")
                    .accessibilityLabel("Dismiss welcome")
                    .accessibilityIdentifier("onboarding.dismiss")
                }

                VStack(alignment: .leading, spacing: 10) {
                    Text("RECOMMENDED FOR FIRST-TIME USERS")
                        .font(.caption2.weight(.bold))
                        .foregroundStyle(.blue)
                    Label("Learn with the TW Hya tutorial", systemImage: "graduationcap.fill")
                        .font(.title3.weight(.semibold))
                    Text("Create a learner workspace, acquire a real ALMA dataset, inspect it, and run imaging tasks one at a time—with an explanation before every step.")
                        .font(.callout)
                        .foregroundStyle(.secondary)
                        .fixedSize(horizontal: false, vertical: true)

                    Button(action: startTutorial) {
                        Label("Start the guided tutorial", systemImage: "arrow.right.circle.fill")
                            .frame(maxWidth: .infinity)
                    }
                    .buttonStyle(.borderedProminent)
                    .controlSize(.large)
                    .accessibilityIdentifier("onboarding.startTutorial")
                }
                .padding(16)
                .frame(maxWidth: .infinity, alignment: .leading)
                .background(Color.blue.opacity(0.08))
                .clipShape(RoundedRectangle(cornerRadius: 10))
                .overlay(RoundedRectangle(cornerRadius: 10).stroke(Color.blue.opacity(0.22)))

                HStack(alignment: .top, spacing: 10) {
                    FirstRunSecondaryPath(
                        icon: "folder.fill",
                        title: "Open my project",
                        detail: "Choose a directory. Workbench will discover supported data, images, tables, and notebooks.",
                        accessibilityID: "onboarding.openProject",
                        action: openProject
                    )

                    FirstRunSecondaryPath(
                        icon: "shippingbox.fill",
                        title: "Explore a demo",
                        detail: "Tour the interface with bundled sample content and no setup or network access.",
                        accessibilityID: "onboarding.openDemo",
                        action: openDemo
                    )
                }

                if let errorMessage {
                    Label(errorMessage, systemImage: "exclamationmark.triangle.fill")
                        .font(.caption)
                        .foregroundStyle(.red)
                        .fixedSize(horizontal: false, vertical: true)
                        .accessibilityIdentifier("onboarding.error")
                }

                Text("Nothing downloads or runs until you review and approve it.")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .frame(maxWidth: .infinity, alignment: .center)
            }
            .padding(28)
            .frame(maxWidth: 650)
            .background(.thickMaterial)
            .clipShape(RoundedRectangle(cornerRadius: 18))
            .shadow(color: .black.opacity(0.35), radius: 28, y: 14)
            .padding(28)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }
}

private struct FirstRunSecondaryPath: View {
    let icon: String
    let title: String
    let detail: String
    let accessibilityID: String
    let action: () -> Void

    var body: some View {
        Button(action: action) {
            HStack(alignment: .top, spacing: 11) {
                Image(systemName: icon)
                    .font(.title3)
                    .foregroundStyle(.blue)
                    .frame(width: 28)
                VStack(alignment: .leading, spacing: 4) {
                    Text(title)
                        .font(.headline)
                    Text(detail)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                        .fixedSize(horizontal: false, vertical: true)
                }
                Spacer(minLength: 4)
                Image(systemName: "chevron.right")
                    .font(.caption.weight(.bold))
                    .foregroundStyle(.secondary)
            }
            .padding(13)
            .frame(maxWidth: .infinity, minHeight: 94, alignment: .topLeading)
            .background(Color(nsColor: .controlBackgroundColor))
            .clipShape(RoundedRectangle(cornerRadius: 10))
        }
        .buttonStyle(.plain)
        .accessibilityIdentifier(accessibilityID)
    }
}

enum BundledTutorialTemplate {
    static func twHyaFirstLookURL() throws -> URL {
        let environment = ProcessInfo.processInfo.environment
        var candidates: [URL] = []

        if let configuredRoot = environment["CASA_RS_TUTORIAL_TEMPLATE_ROOT"],
           !configuredRoot.isEmpty
        {
            candidates.append(URL(fileURLWithPath: configuredRoot, isDirectory: true))
        }
        if let resources = Bundle.main.resourceURL {
            candidates.append(
                resources
                    .appendingPathComponent("tutorials", isDirectory: true)
                    .appendingPathComponent("tw-hya-first-look", isDirectory: true)
            )
        }
        if let repositoryRoot = environment["CASA_RS_REPO_ROOT"],
           !repositoryRoot.isEmpty
        {
            candidates.append(
                URL(fileURLWithPath: repositoryRoot, isDirectory: true)
                    .appendingPathComponent("resources/tutorials/tw-hya-first-look", isDirectory: true)
            )
        }

        for candidate in candidates {
            let manifest = candidate.appendingPathComponent("tutorial.toml")
            let notebook = candidate.appendingPathComponent("tutorial.md")
            if FileManager.default.fileExists(atPath: manifest.path),
               FileManager.default.fileExists(atPath: notebook.path)
            {
                return candidate.standardizedFileURL
            }
        }

        throw BundledTutorialTemplateError.notInstalled(
            candidates.map(\.path)
        )
    }
}

private enum BundledTutorialTemplateError: LocalizedError {
    case notInstalled([String])

    var errorDescription: String? {
        switch self {
        case .notInstalled(let searchedPaths):
            let locations = searchedPaths.isEmpty
                ? "No tutorial resource locations were available."
                : "Checked: \(searchedPaths.joined(separator: ", "))."
            return "The TW Hya tutorial is missing from this app installation. \(locations)"
        }
    }
}
