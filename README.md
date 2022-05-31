# Python Spark transformation


Application which runs [KBC](https://connection.keboola.com/) transformations in Spark cluster provisioned by Data Mechanics.

## Development
 
Clone this repository and init the workspace with following command:

```sh
git clone https://github.com/keboola/python-spark-transformation
cd python-spark-transformation
docker-compose build
docker-compose run --rm dev composer install --no-scripts
```

Create `.env` file with following contents
```env

```

Run the test suite using this command:

```sh
docker-compose run --rm dev composer tests
```
 
# Integration

For information about deployment and integration with KBC, please refer to the [deployment section of developers documentation](https://developers.keboola.com/extend/component/deployment/) 

## License

MIT licensed, see [LICENSE](./LICENSE) file.
