[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-24ddc0f5d75046c5622901739e7c5dd533143b0c8e959d652212380cedb1ea36.svg)](https://classroom.github.com/a/qg4qXfSB)

# Async Messaging System

## Overview

This Java application demonstrates an asynchronous messaging system. It uses multithreading and PostgreSQL databases for message handling. The system includes two main components: `Sender` and `SenderThread`. `Sender` manages connections, sends messages, and handles database interactions. `SenderThread` instances represent individual sender threads.

## Prerequisites

- Java Development Kit (JDK) installed
- PostgreSQL database server accessible
- JDBC executable jar file should be present in your path, which can be downloaded from [JDBC Postgres](https://jdbc.postgresql.org/download/)

## How to Run

1. **Compile the Java files:**

    ```java
    javac Sender.java SenderThread.java
    ```

2. **Run the program:**

    ```java
    java Sender
    ```
## Usage

Messages will automatically be fetched. If you want to send message just type and enter the message.
## Functionality

- **Async Messaging:** The application allows sending and receiving messages asynchronously.
- **Message Handling:** Messages are stored in a PostgreSQL database (`async_messages` table) with sender name, message, sent time, and received time.
- **Multithreading:** `Sender` class manages multiple `SenderThread` instances, each handling a database connection and message sending
