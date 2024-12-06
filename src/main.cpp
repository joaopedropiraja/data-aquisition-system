#include <cstdlib>
#include <iostream>
#include <memory>
#include <utility>
#include <boost/asio.hpp>
#include <unordered_map>
#include <mutex>
#include <regex>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <filesystem>
#include <vector>


using boost::asio::ip::tcp;

const std::string& directory = "logs";

#pragma pack(push, 1)
struct LogRecord {
    char sensor_id[32]; // up to 32 characters
    std::time_t timestamp;
    double value;
};
#pragma pack(pop)

// Function to convert string to std::time_t
std::time_t string_to_time_t(const std::string& time_string) {
    std::tm tm = {};
    std::istringstream ss(time_string);
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");
    return std::mktime(&tm);
}

// Function to convert std::time_t to string
std::string time_t_to_string(std::time_t time) {
    std::tm* tm = std::localtime(&time);
    std::ostringstream ss;
    ss << std::put_time(tm, "%Y-%m-%dT%H:%M:%S");
    return ss.str();
}

class Session: public std::enable_shared_from_this<Session> {

public:
  Session(tcp::socket socket): socket_(std::move(socket)) {}

  void start() {
    this->initializeMutexesFromLogs();
    this->readMessage();
  }

private:
  tcp::socket socket_;
  boost::asio::streambuf buffer_;
  
  static std::unordered_map<std::string, std::shared_ptr<std::shared_mutex>> mutexes;
  static std::mutex mutexes_map_mutex;

  void readMessage() {
    auto self(shared_from_this());
    boost::asio::async_read_until(socket_, buffer_, "\r\n",
      [this, self](boost::system::error_code ec, std::size_t length) {
        if (ec) {
          std::cout << ec.message() << std::endl;
          return;
        }

        std::istream is(&buffer_);
        std::string message(std::istreambuf_iterator<char>(is), {});

        std::string command = message.substr(0, 3);
        std::string params = message.substr(4, message.length() - 1);

        if (command == "LOG") {
          this->handleLog(params);
        } else if (command == "GET") {
          this->handleGet(params);
        } else {
          this->writeMessage("ERROR|INVALID_COMMAND\r\n");
        }
      });
  }

  void writeMessage(const std::string& message) {
    auto self(shared_from_this());
    boost::asio::async_write(socket_, boost::asio::buffer(message),
      [this, self, message](boost::system::error_code ec, std::size_t /*length*/) {
        if (ec) {
          std::cout << ec.message() << std::endl;
          return;          
        }
 
        readMessage();
      });
  }

  void handleLog(std::string params) {    
    std::istringstream iss(params);
    std::string sensorId, timestampStr, valueStr;
    if (
      !std::getline(iss, sensorId, '|') ||
      !std::getline(iss, timestampStr, '|') ||
      !std::getline(iss, valueStr, '|')
    ) {
      this->writeMessage("ERROR|INVALID_SENSOR_ID\r\n");
      return;
    }

    LogRecord log;
    try {
      log.value = std::stod(valueStr);
      log.timestamp = string_to_time_t(timestampStr);
      strcpy(log.sensor_id, sensorId.c_str());
    } catch (...) {
      this->writeMessage("ERROR|INVALID_SENSOR_ID\r\n");
      return;
    }

    std::shared_ptr<std::shared_mutex> sensorMutex;
    {
        std::unique_lock<std::mutex> lock(mutexes_map_mutex);
        auto it = mutexes.find(sensorId);
        if (it == mutexes.end()) {
            sensorMutex = std::make_shared<std::shared_mutex>();
            mutexes[sensorId] = sensorMutex;
        } else {
            sensorMutex = it->second;
        }
    }

    std::filesystem::create_directory(directory);
    std::string logFileName = directory + "/" + sensorId + ".bin";

    {
      std::unique_lock<std::shared_mutex> lock(*sensorMutex);
      
      std::fstream file(logFileName, std::fstream::out | std::fstream::binary | std::fstream::app);
      if (!file) {
        this->writeMessage("ERROR|INVALID_SENSOR_ID\r\n");
        return;
      }

      file.write((char*)&log, sizeof(LogRecord));
      file.close();
    }

    this->writeMessage("SUCCESS\r\n");
  }
  
  void handleGet(std::string params) {
    std::istringstream iss(params);
    std::string sensorId, nStr;
    if (
      !std::getline(iss, sensorId, '|') ||
      !std::getline(iss, nStr, '|')
    ) {
      this->writeMessage("ERROR|INVALID_SENSOR_ID\r\n");
      return;
    }
    int n = std::stoi(nStr);;

    std::shared_ptr<std::shared_mutex> sensorMutex;
    {
      std::unique_lock<std::mutex> lock(mutexes_map_mutex);
      auto it = mutexes.find(sensorId);
      if (it == mutexes.end()) {
        this->writeMessage("ERROR|INVALID_SENSOR_ID\r\n");
        return;
      } else {
          sensorMutex = it->second;
      }
    }

    std::string logFileName = directory + "/" + sensorId + ".bin";
    std::vector<LogRecord> logs;
    {
      std::shared_lock<std::shared_mutex> lock(*sensorMutex);
      std::ifstream file(logFileName, std::ios::binary);
      if (!file) {
          this->writeMessage("ERROR|LOG_FILE_READ_ERROR\r\n");
          return;
      }

      file.seekg(0, std::ios::end);

      int fileSize = file.tellg();
      int logsNum = fileSize / sizeof(LogRecord);
      int logsToRead = std::min(n, logsNum);

      file.seekg(-(logsToRead * sizeof(LogRecord)), std::ios::end);
      logs.resize(logsToRead);
      file.read((char*)(logs.data()), logsToRead * sizeof(LogRecord));

      file.close();
    }

    std::ostringstream response;
    response << logs.size() << ";";
    for (const auto& record : logs) {
        response << time_t_to_string(record.timestamp) << "|" << record.value << ";";
    }
    response << "\r\n";

    this->writeMessage(response.str());
  }

  void initializeMutexesFromLogs() {
    std::filesystem::create_directory(directory);

    for (const auto& entry : std::filesystem::directory_iterator(directory)) {
      if (entry.is_regular_file()) {
        std::string fileName = entry.path().filename().string();
        std::string sensorId = fileName.substr(0, fileName.find_last_of("."));
      
        if (mutexes.find(sensorId) == mutexes.end()) {
          mutexes[sensorId] = std::make_shared<std::shared_mutex>();
        }
      }
    }
  }  
};

class Server {
public:
  Server(boost::asio::io_context& io_context, short port)  
    : acceptor_(io_context, tcp::endpoint(tcp::v4(), port)) 
  {
    
    this->accept();
  }

private:
  tcp::acceptor acceptor_;

  void accept() {
    acceptor_.async_accept(
      [this](boost::system::error_code ec, tcp::socket socket) {
        if (ec) {
          std::cout << ec.message() << std::endl;
          return;
        }        

        std::make_shared<Session>(std::move(socket))->start();
        this->accept();
      });
  }
};

std::unordered_map<std::string, std::shared_ptr<std::shared_mutex>> Session::mutexes;
std::mutex Session::mutexes_map_mutex;

int main(int argc, char *argv[]) {
  if (argc != 2) {
    std::cerr << "Usage: <port>\n";
    return EXIT_FAILURE;
  }

  boost::asio::io_context io_context;
  Server s(io_context, std::atoi(argv[1]));

  io_context.run();

  return EXIT_SUCCESS;
}