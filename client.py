import argparse
import grpc
from typing import Iterable
import csv

import bookstore_pb2
import bookstore_pb2_grpc


def add_book(stub, title: str, author: str, price: float, quantity: int):
    req = bookstore_pb2.AddBookRequest(
        title=title, author=author, price=price, quantity=quantity
    )
    resp = stub.AddBook(req)
    print(f"✅ Added book: {title!r} (id={resp.id})")
    return resp.id


def get_book(stub, book_id: str):
    try:
        book = stub.GetBook(bookstore_pb2.GetBookRequest(id=book_id))
        print_book(book)
    except grpc.RpcError as e:
        print(f"❌ GetBook failed: {e.code().name} - {e.details()}")


def list_books(stub, author: str = "", only_in_stock: bool = False):
    req = bookstore_pb2.ListBooksRequest(author=author, only_in_stock=only_in_stock)
    stream: Iterable[bookstore_pb2.Book] = stub.ListBooks(req)
    found = False
    for book in stream:
        found = True
        print_book(book)
    if not found:
        print("(no books matched)")


def upload_books(stub, file: str = ""):
    if file:
        req_iter = iter_add_requests_from_csv(file)
    else:
        req_iter = iter_sample_requests()

    print("📤 Uploading books (bidi streaming)...")
    for res in stub.UploadBooks(req_iter):
        if res.error:
            print(f"❌ error: {res.error}")
        else:
            print(f"✅ created id={res.id}")


def iter_add_requests_from_csv(path: str):
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, fieldnames=["title", "author", "price", "qty"])
        for row in reader:
            yield bookstore_pb2.AddBookRequest(
                title=row["title"],
                author=row["author"],
                price=float(row["price"]),
                quantity=int(row["qty"]),
            )


def iter_sample_requests():
    samples = [
        ("The Pragmatic Programmer", "Andy Hunt", 650.0, 4),
        ("Refactoring", "Martin Fowler", 720.0, 3),
        ("Python Tricks", "Dan Bader", 430.0, 5),
        ("", "No Name", 100.0, 1),  # triggers error (empty title)
    ]
    for t, a, p, q in samples:
        yield bookstore_pb2.AddBookRequest(title=t, author=a, price=p, quantity=q)


def print_book(book: bookstore_pb2.Book):
    ts = book.created_at.ToDatetime().isoformat()
    print(
        f"- [{book.id}] {book.title} — {book.author} | "
        f"₹{book.price:.2f} | qty={book.quantity} | created_at={ts}"
    )


def main():
    parser = argparse.ArgumentParser(description="Bookstore gRPC client")
    parser.add_argument("--addr", default="localhost:50051", help="server address")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # add
    p_add = sub.add_parser("add", help="Add a new book")
    p_add.add_argument("--title", required=True)
    p_add.add_argument("--author", required=True)
    p_add.add_argument("--price", type=float, required=True)
    p_add.add_argument("--qty", type=int, required=True)

    # get
    p_get = sub.add_parser("get", help="Get a book by id")
    p_get.add_argument("--id", required=True)

    # list
    p_list = sub.add_parser("list", help="List (stream) books")
    p_list.add_argument("--author", default="", help="filter by author (optional)")
    p_list.add_argument("--only-in-stock", action="store_true")

    # bulk (bidi)
    p_bulk = sub.add_parser("bulk", help="Bulk upload via bidirectional streaming")
    p_bulk.add_argument("--file", default="", help="CSV (title,author,price,qty)")

    args = parser.parse_args()

    with grpc.insecure_channel(args.addr) as channel:
        stub = bookstore_pb2_grpc.BookServiceStub(channel)

        if args.cmd == "add":
            add_book(stub, args.title, args.author, args.price, args.qty)
        elif args.cmd == "get":
            get_book(stub, args.id)
        elif args.cmd == "list":
            list_books(stub, args.author, args.only_in_stock)
        elif args.cmd == "bulk":
            upload_books(stub, args.file)


if __name__ == "__main__":
    main()
