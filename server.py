from concurrent import futures
import grpc
import uuid
from datetime import datetime, timezone
import time

from google.protobuf.timestamp_pb2 import Timestamp

import bookstore_pb2
import bookstore_pb2_grpc


# ---- Simple In-Memory Store ----
class InMemoryStore:
    def __init__(self):
        self._books = {}

    def add(self, book: bookstore_pb2.Book):
        self._books[book.id] = book

    def get(self, book_id: str):
        return self._books.get(book_id)

    def all(self):
        return list(self._books.values())


# ---- Service Implementation ----
class BookService(bookstore_pb2_grpc.BookServiceServicer):
    def __init__(self, store: InMemoryStore):
        self.store = store

    # ---------- Unary ----------
    def AddBook(self, request, context):
        if not request.title.strip():
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "title required")
        if not request.author.strip():
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "author required")

        book_id = str(uuid.uuid4())
        ts = Timestamp(); ts.FromDatetime(datetime.now(timezone.utc))
        book = bookstore_pb2.Book(
            id=book_id,
            title=request.title,
            author=request.author,
            price=request.price,
            quantity=request.quantity,
            created_at=ts,
        )
        self.store.add(book)
        return bookstore_pb2.AddBookResponse(id=book_id)

    def GetBook(self, request, context):
        book = self.store.get(request.id)
        if book is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"Book {request.id} not found")
        return book

    # ---------- Server-streaming ----------
    def ListBooks(self, request, context):
        for book in self.store.all():
            if request.author and book.author.lower() != request.author.lower():
                continue
            if request.only_in_stock and book.quantity <= 0:
                continue
            # Uncomment to visibly demonstrate streaming:
            # time.sleep(1)
            yield book

    # ---------- Bidirectional streaming ----------
    def UploadBooks(self, request_iterator, context):
        """
        For each AddBookRequest, validate and immediately yield AddBookResult.
        """
        for req in request_iterator:
            title = (req.title or "").strip()
            author = (req.author or "").strip()

            if not title:
                yield bookstore_pb2.AddBookResult(error="title required"); continue
            if not author:
                yield bookstore_pb2.AddBookResult(error="author required"); continue
            if req.price < 0:
                yield bookstore_pb2.AddBookResult(error="price must be >= 0"); continue
            if req.quantity < 0:
                yield bookstore_pb2.AddBookResult(error="quantity must be >= 0"); continue

            book_id = str(uuid.uuid4())
            ts = Timestamp(); ts.FromDatetime(datetime.now(timezone.utc))
            self.store.add(bookstore_pb2.Book(
                id=book_id, title=title, author=author,
                price=float(req.price), quantity=int(req.quantity),
                created_at=ts,
            ))
            yield bookstore_pb2.AddBookResult(id=book_id)


# ---- Server Bootstrap ----
def serve(host="0.0.0.0", port=50051):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    bookstore_pb2_grpc.add_BookServiceServicer_to_server(BookService(InMemoryStore()), server)
    server.add_insecure_port(f"{host}:{port}")
    server.start()
    print(f"[gRPC] BookService running on {host}:{port}")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
