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
        auto params = view["Params"].get_document();

//        TD<decltype(params)> pTyp;

        if (oper == "database") {
            return this->db(params.view());
        }
        if (oper == "find") {
            return this->find(params.view());
        }
        return Status::FAIL;
    }

private:
    Status db(bsoncxx::document::view params) {
        this->_dbName = bsoncxx::string::to_string(params["Name"].get_utf8().value);
        this->reset();
        return Status::OK;
    }

    Status find(bsoncxx::document::view params) {
        auto query = params["Query"].get_document();
        mongocxx::cursor cur = this->_collection.find(query.view());
        for(auto& doc : cur) {
            std::cout << bsoncxx::to_json(doc) << std::endl;
        }
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
//        if (line == "db") {
//            std::getline(std::cin, line);
//            db = client[line];
//            std::cout << "db " << line << " OK" << std::endl;
//        }
//        else if (line == "collection") {
//            std::getline(std::cin, line);
//            collection = db[line];
//            std::cout << "collection " << line << " OK" << std::endl;
//        }
//        else if (line == "find") {
//            std::getline(std::cin, line);
//            mongocxx::cursor cur = collection.find(bsoncxx::from_json(line));
//            for(auto& doc : cur) {
//                std::cout << bsoncxx::to_json(doc) << std::endl;
//            }
//            std::cout << "OK" << std::endl;
//        }
//        else {
//            std::cout << "Unknown " << line << " FAIL" << std::endl;
//        }
    }
}
