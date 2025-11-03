# Documentation Style Guide

This guide provides principles for writing clear, concise, and helpful documentation for the `otlp2parquet` project. Our goal is to combine the elegant simplicity of E.B. White with the user-focused design of great technical docs, like those from Stripe.

## Core Principles

These principles are the foundation of our writing style.

*   **Be Clear and Direct**: Use simple, declarative sentences. Get straight to the point. Avoid jargon where possible, and explain it when it's necessary.

*   **Omit Needless Words**: Every word should serve a purpose. If a sentence can be shortened without losing its meaning, shorten it. We are not writing marketing copy; we are providing essential information.

*   **Use the Active Voice**: The active voice is more direct and easier to understand.
    *   **Do:** "The function sends a batch of logs."
    *   **Don't:** "A batch of logs is sent by the function."

*   **Write with a Human Voice**: Write as if you are a knowledgeable colleague patiently explaining something to a teammate. It should be helpful and approachable, but not overly chatty or informal.

## Structure & Presentation

How we structure our documentation is as important as the words we use.

*   **Focus on User Goals**: Organize documents around what the user is trying to achieve. A title should reflect a user's goal (e.g., "Querying Data with DuckDB") rather than just naming a feature.

*   **Lead with the "Why"**: Before explaining *what* something is, briefly explain *why* the user should care. For example, when introducing batching, first explain that it saves money and improves performance.

*   **Make it Scannable**: Users often scan documents before reading them. Use clear, descriptive headings, short paragraphs, and bullet points to make information easy to find.

*   **Code is a First-Class Citizen**:
    *   Provide complete, copy-pasteable code examples.
    *   Explain what the code does and why it works.
    *   Use comments in code sparingly, only to explain the *why* behind a specific line, not the *what*.

## Rust-Specific Guidelines

When writing Rust code and its accompanying documentation, adhere to these best practices:

*   **Idiomatic Rust**: Embrace Rust's ownership, borrowing, and concurrency models. Use `Result` and `Option` for robust error handling.
*   **Clear Code**: Prioritize readability and maintainability. Avoid overly clever solutions.
*   **API Documentation**: Use `///` comments for all public functions, structs, and enums. Provide clear explanations and examples where appropriate.

## Tone of Voice

*   **Confident and Knowledgeable**: We are the experts on this tool. Present information with confidence.
*   **Empathetic and Patient**: Our users may be new to OpenTelemetry, Parquet, or serverless environments. Anticipate their questions and provide clear guidance.
*   **Direct and Unfussy**: Avoid hyperbole and marketing language. Be straightforward and honest, even about the project's limitations or experimental nature.
