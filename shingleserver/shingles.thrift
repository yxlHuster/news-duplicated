service shingleService {
list<string> getShingleString(1:required string contents),
list<i64> getShingleLong(1:required string contents),
list<i64> getSimDocuments(1:required string contents),
}
