import os
from collections.abc import Iterator
from typing import Any

import dlt
from dlt.sources.helpers.rest_client import PageData, RESTClient
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator

os.environ["EXTRACT__WORKERS"] = "6"
os.environ["DATA_WRITER__BUFFER_MAX_ITEMS"] = "10000"
os.environ["EXTRACT__DATA_WRITER__FILE_MAX_ITEMS"] = "20000"

os.environ["NORMALIZE__WORKERS"] = "4"
os.environ["NORMALIZE__DATA_WRITER__BUFFER_MAX_ITEMS"] = "20000"
os.environ["NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS"] = "20000"

os.environ["LOAD__WORKERS"] = "4"


@dlt.source(name="jaffle_shop_source")
def jaffle_shop_source() -> tuple[Any, Any, Any]:
    @dlt.resource(name="customers", parallelized=True)
    def get_customers() -> Iterator[PageData[Any]]:
        client = RESTClient(
            base_url="https://jaffle-shop.scalevector.ai/api/v1",
            paginator=HeaderLinkPaginator(),
        )
        params = {"page_size": 5000}
        yield from client.paginate("/customers", params=params)

    @dlt.resource(name="orders", parallelized=True)
    def get_orders() -> Iterator[PageData[Any]]:
        client = RESTClient(
            base_url="https://jaffle-shop.scalevector.ai/api/v1",
            paginator=HeaderLinkPaginator(),
        )
        params = {"page_size": 5000}
        yield from client.paginate("/orders", params=params)

    @dlt.resource(name="products", parallelized=True)
    def get_products() -> Iterator[PageData[Any]]:
        client = RESTClient(
            base_url="https://jaffle-shop.scalevector.ai/api/v1",
            paginator=HeaderLinkPaginator(),
        )
        params = {"page_size": 5000}
        yield from client.paginate("/products", params=params)

    return get_customers, get_orders, get_products


pipeline = dlt.pipeline(
    pipeline_name="jaffle_shop_pipeline_optimized",
    destination="duckdb",
    dataset_name="jaffle_shop",
    dev_mode=True,
)

load_info = pipeline.run(jaffle_shop_source())
print(pipeline.last_trace)
