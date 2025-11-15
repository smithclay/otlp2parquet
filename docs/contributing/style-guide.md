# Documentation Style Guide

This guide provides principles for writing clear, concise, and helpful documentation for the `otlp2parquet` project.

Our goal is to combine the elegant simplicity of E.B. White with the user-focused design of great technical docs.

## Core Principles

*   **Be Clear and Direct**: Use simple, declarative sentences. Get straight to the point. Explain jargon when it's necessary.
*   **Omit Needless Words**: Every word should serve a purpose. If a sentence can be shortened without losing its meaning, shorten it.
*   **Use the Active Voice**: The active voice is more direct and easier to understand.
    *   **Do:** "The function sends a batch of logs."
    *   **Don't:** "A batch of logs is sent by the function."
*   **Write with a Human Voice**: Be helpful and approachable, but not overly chatty or informal.

## Structure and Presentation

*   **Focus on User Goals**: Organize documents around what the user is trying to achieve. A title should reflect a user's goal (e.g., "Querying Data with DuckDB").
*   **Lead with the "Why"**: Before explaining *what* something is, briefly explain *why* the user should care.
*   **Make it Scannable**: Use clear headings, short paragraphs, and bullet points to make information easy to find.
*   **Code is a First-Class Citizen**: Provide complete, copy-pasteable code examples. Explain what the code does and why it works. Use comments in code sparingly to explain the *why*, not the *what*.

## Rust-Specific Guidelines

*   **Idiomatic Rust**: Embrace Rust's ownership, borrowing, and concurrency models. Use `Result` and `Option` for robust error handling.
*   **Clear Code**: Prioritize readability and maintainability.
*   **API Documentation**: Use `///` comments for all public functions, structs, and enums. Provide clear explanations and examples.

## Tone of Voice

*   **Confident and Knowledgeable**: Present information with confidence.
*   **Empathetic and Patient**: Anticipate user questions and provide clear guidance.
*   **Direct and Unfussy**: Avoid hyperbole and marketing language. Be straightforward and honest.
