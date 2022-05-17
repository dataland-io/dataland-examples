# Quickstart Example

```sh
npm install
dataland deploy
```

This is the "Hello world" of Dataland. It consists of the following constructs:

- A "greetings" table which has two string columns - "name" and "greeting".
- A "hello" worker which will always write `Hello, {name}!` to the "greeting" column.

For example, if a row is added with "world" as the name,
the hello worker will write "Hello, world!" to the greeting column.
