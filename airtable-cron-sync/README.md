# Quickstart Example

```sh
npm install
dataland deploy
```

The starting state of the quickstart example in the Dataland Docs:
<https://docs.dataland.io/quickstart.html>

This is the "Hello world" of Dataland. It consists of the following constructs:

- A "greetings" table which has two columns - "name" and "greeting".
- A "hello" worker which will write `Hello, {name}!` to the greeting column for every row in the table.

For example, if a row is added with "world" as the name,
the hello worker will respond by writing "Hello, world!" to the greeting column.
