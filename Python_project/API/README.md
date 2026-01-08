* API: Application Programming Interface
    * It's a set of rules that allows different software or systems to communicate with each other

    * Live on some kind of server and manipulate some kind of database

    * Can handle different requests

    * Examples:

        * A library API (like Python’s os module) lets your program talk to the operating system.

        * A database API (like JDBC) lets your app talk to a database.

        * A web API lets apps communicate over the internet.

* REST API: A REST API is a specific type of web API that follows REST architectural principles — a standardized way of structuring communication over HTTP.

| HTTP Method | Purpose              | Example Endpoint    |
| ----------- | -------------------- | ------------------- |
| `GET`       | Retrieve data        | `GET /users`        |
| `POST`      | Create new data      | `POST /users`       |
| `PUT`       | Update existing data | `PUT /users/123`    |
| `DELETE`    | Delete data          | `DELETE /users/123` |

* Using '?' after the route name to query additional variable

* To test API, use POSTMAN