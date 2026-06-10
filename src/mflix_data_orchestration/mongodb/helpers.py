"""Mongo database source helpers"""

from itertools import islice
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
    Iterable,
    Mapping,
)

import dlt
from bson.decimal128 import Decimal128
from bson.objectid import ObjectId
from bson.regex import Regex
from bson.timestamp import Timestamp
from dlt.common import logger
from dlt.common.configuration.specs import BaseConfiguration, configspec
from dlt.common.data_writers import TDataItemFormat
from dlt.common.time import ensure_pendulum_datetime
from dlt.common.typing import TDataItem
from dlt.common.utils import map_nested_in_place
from pendulum import _datetime
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.collection import Collection
from pymongo.cursor import Cursor
from pymongo.helpers_shared import _fields_list_to_dict


if TYPE_CHECKING:
    TMongoClient = MongoClient[Any]
    TCollection = Collection[Any]
    TCursor = Cursor[Any]
else:
    TMongoClient = Any
    TCollection = Any
    TCursor = Any

try:
    import pymongoarrow  # type: ignore

    PYMONGOARROW_AVAILABLE = True
except ImportError:
    PYMONGOARROW_AVAILABLE = False


class CollectionLoader:
    def __init__(
        self,
        client: TMongoClient,
        collection: TCollection,
        chunk_size: int,
        incremental: Optional[dlt.sources.incremental[Any]] = None,
    ) -> None:
        self.client = client
        self.collection = collection
        self.incremental = incremental
        self.chunk_size = chunk_size

        if incremental:
            self.cursor_field = incremental.cursor_path
            self.last_value = incremental.last_value
        else:
            self.cursor_column = None
            self.last_value = None

    @property
    def _sort_op(self) -> List[Optional[Tuple[str, int]]]:
        if not self.incremental or not self.last_value:
            return []

        if (
            self.incremental.row_order == "asc"
            and self.incremental.last_value_func is max
        ) or (
            self.incremental.row_order == "desc"
            and self.incremental.last_value_func is min
        ):
            return [(self.cursor_field, ASCENDING)]

        elif (
            self.incremental.row_order == "asc"
            and self.incremental.last_value_func is min
        ) or (
            self.incremental.row_order == "desc"
            and self.incremental.last_value_func is max
        ):
            return [(self.cursor_field, DESCENDING)]

        return []

    @property
    def _filter_op(self) -> Dict[str, Any]:
        """Build a filtering operator.

        Includes a field and the filtering condition for it.

        Returns:
            Dict[str, Any]: A dictionary with the filter operator.
        """
        if not (self.incremental and self.last_value):
            return {}

        filt = {}
        if self.incremental.last_value_func is max:
            filt = {self.cursor_field: {"$gte": self.last_value}}
            if self.incremental.end_value:
                filt[self.cursor_field]["$lt"] = self.incremental.end_value

        elif self.incremental.last_value_func is min:
            filt = {self.cursor_field: {"$lte": self.last_value}}
            if self.incremental.end_value:
                filt[self.cursor_field]["$gt"] = self.incremental.end_value

        return filt

    def _projection_op(
        self, projection: Optional[Union[Mapping[str, Any], Iterable[str]]]
    ) -> Optional[Dict[str, Any]]:
        """Build a projection operator.

        Args:
            projection (Optional[Union[Mapping[str, Any], Iterable[str]]]): A tuple of fields to include or a dict specifying fields to include or exclude.
            The incremental `primary_key` needs to be handle differently for inclusion
            and exclusion projections.

        Returns:
            Tuple[str, ...] | Dict[str, Any]: A tuple or dictionary with the projection operator.
        """
        if projection is None:
            return None

        projection_dict = dict(_fields_list_to_dict(projection, "projection"))

        if self.incremental:
            # this is an inclusion projection
            if any(v == 1 for v in projection_dict.values()):
                # ensure primary_key is included
                projection_dict.update(m={self.incremental.primary_key: 1})
            # this is an exclusion projection
            else:
                try:
                    # ensure primary_key isn't excluded
                    projection_dict.pop(self.incremental.primary_key)  # type: ignore
                except KeyError:
                    pass  # primary_key was properly not included in exclusion projection
                else:
                    dlt.common.logger.warn(
                        f"Primary key `{self.incremental.primary_key}` was removed from exclusion projection"
                    )

        return projection_dict

    def _limit(self, cursor: Cursor, limit: Optional[int] = None) -> TCursor:  # type: ignore
        """Apply a limit to the cursor, if needed.

        Args:
            cursor (Cursor): The cursor to apply the limit to.
            limit (Optional[int]): The number of documents to load.

        Returns:
            Cursor: The cursor with the limit applied (if given).
        """
        if limit not in (0, None):
            if self.incremental is None or self.incremental.last_value_func is None:
                logger.warning(
                    "Using limit without ordering - results may be inconsistent."
                )

            cursor = cursor.limit(abs(limit))

        return cursor

    def load_documents(
        self,
        filter_: Dict[str, Any],
        limit: Optional[int] = None,
        projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
    ) -> Iterator[TDataItem]:
        """Construct the query and load the documents from the collection.

        Args:
            filter_ (Dict[str, Any]): The filter to apply to the collection.
            limit (Optional[int]): The number of documents to load.
            projection (Optional[Union[Mapping[str, Any], Iterable[str]]]): The projection to select fields to create the Cursor.

        Yields:
            Iterator[TDataItem]: An iterator of the loaded documents.
        """
        filter_op = self._filter_op
        _raise_if_intersection(filter_op, filter_)
        filter_op.update(filter_)

        projection_op = self._projection_op(projection)

        cursor = self.collection.find(filter=filter_op, projection=projection_op)
        if self._sort_op:
            cursor = cursor.sort(self._sort_op)

        cursor = self._limit(cursor, limit)

        while docs_slice := list(islice(cursor, self.chunk_size)):
            yield map_nested_in_place(convert_mongo_objs, docs_slice)


class CollectionLoaderParallel(CollectionLoader):
    def _get_document_count(self) -> int:
        return self.collection.count_documents(filter=self._filter_op)

    def _create_batches(self, limit: Optional[int] = None) -> List[Dict[str, int]]:
        doc_count = self._get_document_count()
        if limit:
            doc_count = min(doc_count, abs(limit))

        batches = []
        left_to_load = doc_count

        for sk in range(0, doc_count, self.chunk_size):
            batches.append(dict(skip=sk, limit=min(self.chunk_size, left_to_load)))
            left_to_load -= self.chunk_size

        return batches

    def _get_cursor(
        self,
        filter_: Dict[str, Any],
        projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
    ) -> TCursor:
        """Get a reading cursor for the collection.

        Args:
            filter_ (Dict[str, Any]): The filter to apply to the collection.
            projection (Optional[Union[Mapping[str, Any], Iterable[str]]]): The projection to select fields to create the Cursor.

        Returns:
            Cursor: The cursor for the collection.
        """
        filter_op = self._filter_op
        _raise_if_intersection(filter_op, filter_)
        filter_op.update(filter_)

        projection_op = self._projection_op(projection)

        cursor = self.collection.find(filter=filter_op, projection=projection_op)
        if self._sort_op:
            cursor = cursor.sort(self._sort_op)

        return cursor

    @dlt.defer
    def _run_batch(self, cursor: TCursor, batch: Dict[str, int]) -> TDataItem:
        cursor = cursor.clone()

        data = []
        for document in cursor.skip(batch["skip"]).limit(batch["limit"]):
            data.append(map_nested_in_place(convert_mongo_objs, document))

        return data

    def _get_all_batches(
        self,
        filter_: Dict[str, Any],
        limit: Optional[int] = None,
        projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
    ) -> Iterator[TDataItem]:
        """Load all documents from the collection in parallel batches.

        Args:
            filter_ (Dict[str, Any]): The filter to apply to the collection.
            limit (Optional[int]): The maximum number of documents to load.
            projection (Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
        ) -> Iterator[TDataItem]:
            """
            batches = self._create_batches(limit=limit)
            cursor = self._get_cursor(filter_=filter_, projection=projection)
            for batch in batches:
                yield self._run_batch(
                    cursor=cursor,
                    batch=batch,
                    pymongoarrow_schema=pymongoarrow_schema,
                )

    def _get_cursor(
        self,
        filter_: Dict[str, Any],
        projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
    ) -> TCursor:
        """Get a reading cursor for the collection.

        Args:
            filter_ (Dict[str, Any]): The filter to apply to the collection.
            projection (Optional[Union[Mapping[str, Any], Iterable[str]]]): The projection to select fields to create the Cursor.

        Returns:
            Cursor: The cursor for the collection.
        """
        filter_op = self._filter_op
        _raise_if_intersection(filter_op, filter_)
        filter_op.update(filter_)

        projection_op = self._projection_op(projection)

        cursor = self.collection.find_raw_batches(
            filter_op, batch_size=self.chunk_size, projection=projection_op
        )
        if self._sort_op:
            cursor = cursor.sort(self._sort_op)  # type: ignore

        cursor = self._limit(cursor, limit)  # type: ignore

        context = PyMongoArrowContext.from_schema(
            schema=pymongoarrow_schema, codec_options=self.collection.codec_options
        )
        for batch in cursor:
            process_bson_stream(batch, context)
            table = context.finish()
            yield convert_arrow_columns(table)


class CollectionArrowLoaderParallel(CollectionLoaderParallel):
    """
    Mongo DB collection parallel loader, which uses
    Apache Arrow for data processing.
    """

    def load_documents(
        self,
        filter_: Dict[str, Any],
        limit: Optional[int] = None,
        projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
        pymongoarrow_schema: Any = None,
    ) -> Iterator[TDataItem]:
        """Load documents from the collection in parallel.

        Args:
            filter_ (Dict[str, Any]): The filter to apply to the collection.
            limit (Optional[int]): The maximum number of documents to load.
            projection (Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
        ) -> Iterator[TDataItem]:
            yield from self._get_all_batches(
                limit=limit,
                filter_=filter_,
                projection=projection,
                pymongoarrow_schema=pymongoarrow_schema,
            )

    def _get_all_batches(
        self,
        filter_: Dict[str, Any],
        limit: Optional[int] = None,
        projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
        pymongoarrow_schema: Any = None,
    ) -> Iterator[TDataItem]:
        """Load all documents from the collection in parallel batches.

        Args:
            filter_ (Dict[str, Any]): The filter to apply to the collection.
            limit (Optional[int]): The maximum number of documents to load.
            projection (Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
        ) -> Iterator[TDataItem]:
            """
            yield from self._get_all_batches(
                limit=limit,
                filter_=filter_,
                projection=projection,
                pymongoarrow_schema=pymongoarrow_schema,
            )

    def _get_cursor(
        self,
        filter_: Dict[str, Any],
        projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
    ) -> TCursor:
        """Get a reading cursor for the collection.

        Args:
            filter_ (Dict[str, Any]): The filter to apply to the collection.
            projection (Optional[Union[Mapping[str, Any], Iterable[str]]]): The projection to select fields to create the Cursor.

        Returns:
            Cursor: The cursor for the collection.
        """
        for document in self._get_all_batches(
            limit=limit,
            filter_=filter_,
            projection=projection,
            pymongoarrow_schema=pymongoarrow_schema,
        )

    def load_documents(
        self,
        filter_: Dict[str, Any],
        limit: Optional[int] = None,
        projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
    ) -> Iterator[TDataItem]:
        """Load documents from the collection in parallel.

        Args:
            filter_ (Dict[str, Any]): The filter to apply to the collection.
            limit (Optional[int]): The maximum number of documents to load.
            projection (Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
        ) -> Iterator[TDataItem]:
            for document in self._get_all_batches(
                limit=limit, filter_=filter_, projection=projection
            )


class CollectionArrowLoaderParallel(CollectionLoaderParallel):
    def _get_document_count(self) -> int:
        return self.collection.count_documents(filter=self._filter_op)


# NOTE: file truncated in generation for brevity. Full file moved from original location.
