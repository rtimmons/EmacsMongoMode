#include <cstring>
#include <fstream>
#include <iostream>
#include <optional>
#include <string>

#include <bsoncxx/document/element.hpp>
#include <bsoncxx/string/to_string.hpp>

#include <mongocxx/instance.hpp>
#include <mongocxx/cursor.hpp>
#include <mongocxx/client.hpp>
#include <bsoncxx/json.hpp>

template <class T>
class TD;

enum class Status {
    OK,
    FAIL
};

class state {
    mongocxx::database _db;
    mongocxx::collection _collection;
    mongocxx::client _client;


    std::string _uri = "mongodb://localhost:27017";
    std::string _dbName = "test";
    std::string _collName = "test";

    std::ostream& _out;

public:
    explicit state(std::ostream &_out) : _out(_out) {
        this->reset();
    }

    Status op(const std::string& json) {
        auto doc = bsoncxx::from_json(json);
        auto view = doc.view();
        std::string oper = bsoncxx::string::to_string(view["Oper"].get_utf8().value);

        if (oper == "setDatabase") {
            this->_dbName = bsoncxx::string::to_string(view["Params"]["Value"].get_utf8().value);
            this->reset();
            return Status::OK;
        }
        if (oper == "setCollection") {
            this->_collName = bsoncxx::string::to_string(view["Params"]["Value"].get_utf8().value);
            this->reset();
            return Status::OK;
        }
        if (oper == "setUri") {
            this->_uri = bsoncxx::string::to_string(view["Params"]["Value"].get_utf8().value);
            this->reset();
            return Status::OK;
        }

        if (oper == "find") {
            return this->find(view["Params"].get_document().view());
        }
        if (oper == "runCommand") {
            return this->runCommand(view["Params"].get_document().view());
        }
        if (oper == "Ping") {
            return this->runCommand(bsoncxx::from_json("{\"Command\": {\"ping\":1}}"));
        }
        if (oper == "InsertOne") {
            return this->insertOne(view["Params"].get_document().view());
        }

        return Status::FAIL;
    }

private:
    Status insertOne(bsoncxx::document::view params) {
        auto res = this->_collection.insert_one(params["Document"].get_document().view());
        return Status::OK;
    }

    Status find(bsoncxx::document::view params) {
        auto query = params["Query"].get_document();
        mongocxx::cursor cur = this->_collection.find(query.view());
        for(auto& doc : cur) {
            this->_out << bsoncxx::to_json(doc) << std::endl;
        }
        return Status::OK;
    }

    Status runCommand(bsoncxx::document::view params) {
        auto command = params["Command"].get_document();
        auto out = this->_db.run_command(command.view());
        this->_out << bsoncxx::to_json(out) << std::endl;
        return Status::OK;
    }

    void reset() {
        this->_client = mongocxx::client{mongocxx::uri{this->_uri}};
        this->_db = this->_client[_dbName];
        this->_collection = this->_db[_collName];
    }
};


int main(int argc, char** argv) {
    mongocxx::instance instance{}; // This should be done only once.

    state s{std::cout};

    for (std::string line; std::getline(std::cin, line);) {
        s.op(line);
    }
}
