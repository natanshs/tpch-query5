#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>
#include <unordered_map> 

// Function to parse command line arguments
bool parseArgs(int argc, char* argv[], std::string& r_name, std::string& start_date,
               std::string& end_date, int& num_threads, std::string& table_path,
               std::string& result_path) {
    num_threads = 1;

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--r_name" && i + 1 < argc) {
            r_name = argv[++i];
        } else if (arg == "--start_date" && i + 1 < argc) {
            start_date = argv[++i];
        } else if (arg == "--end_date" && i + 1 < argc) {
            end_date = argv[++i];
        } else if (arg == "--threads" && i + 1 < argc) {
            num_threads = std::stoi(argv[++i]);
        } else if (arg == "--table_path" && i + 1 < argc) {
            table_path = argv[++i];
        } else if (arg == "--result_path" && i + 1 < argc) {
            result_path = argv[++i];
        } else {
            std::cerr << "Unknown or malformed argument: " << arg << std::endl;
            return false;
        }
    }

    if (r_name.empty() || start_date.empty() || end_date.empty() ||
        table_path.empty() || result_path.empty()) {
        std::cerr << "Missing required argument." << std::endl;
        return false;
    }
    return true;
}

// Function to read TPCH data from the specified paths
bool readOneTable(const std::string& filepath, const std::vector<std::string>& columnNames,
                   std::vector<std::map<std::string, std::string>>& outData) {
    std::ifstream file(filepath);
    if (!file.is_open()) {
        std::cerr << "Failed to open file: " << filepath << std::endl;
        return false;
    }
    std::string line;
    while (std::getline(file, line)) {
        std::map<std::string, std::string> row;
        std::stringstream ss(line);
        std::string field;
        size_t colIndex = 0;
        while (std::getline(ss, field, '|') && colIndex < columnNames.size()) {
            row[columnNames[colIndex]] = field;
            colIndex++;
        }
        outData.push_back(row);
    }
    file.close();
    return true;
}
bool readLineitemSelective(const std::string& filepath, std::vector<std::map<std::string, std::string>>& outData);

bool readTPCHData(const std::string& table_path,
                   std::vector<std::map<std::string, std::string>>& customer_data,
                   std::vector<std::map<std::string, std::string>>& orders_data,
                   std::vector<std::map<std::string, std::string>>& lineitem_data,
                   std::vector<std::map<std::string, std::string>>& supplier_data,
                   std::vector<std::map<std::string, std::string>>& nation_data,
                   std::vector<std::map<std::string, std::string>>& region_data) {

    std::vector<std::string> customerCols = {"c_custkey","c_name","c_address","c_nationkey",
                                              "c_phone","c_acctbal","c_mktsegment","c_comment"};
    std::vector<std::string> ordersCols = {"o_orderkey","o_custkey","o_orderstatus","o_totalprice",
                                            "o_orderdate","o_orderpriority","o_clerk",
                                            "o_shippriority","o_comment"};
    std::vector<std::string> lineitemCols = {"l_orderkey","l_partkey","l_suppkey","l_linenumber",
                                              "l_quantity","l_extendedprice","l_discount","l_tax",
                                              "l_returnflag","l_linestatus","l_shipdate",
                                              "l_commitdate","l_receiptdate","l_shipinstruct",
                                              "l_shipmode","l_comment"};
    std::vector<std::string> supplierCols = {"s_suppkey","s_name","s_address","s_nationkey",
                                              "s_phone","s_acctbal","s_comment"};
    std::vector<std::string> nationCols = {"n_nationkey","n_name","n_regionkey","n_comment"};
    std::vector<std::string> regionCols = {"r_regionkey","r_name","r_comment"};

    bool ok = true;
    ok &= readOneTable(table_path + "/customer.tbl", customerCols, customer_data);
    ok &= readOneTable(table_path + "/orders.tbl", ordersCols, orders_data);
    ok &= readLineitemSelective(table_path + "/lineitem.tbl", lineitem_data);
    ok &= readOneTable(table_path + "/supplier.tbl", supplierCols, supplier_data);
    ok &= readOneTable(table_path + "/nation.tbl", nationCols, nation_data);
    ok &= readOneTable(table_path + "/region.tbl", regionCols, region_data);

    return ok;
}

// Function to execute TPCH Query 5 using multithreading

bool executeQuery5(const std::string& r_name, const std::string& start_date, const std::string& end_date,
                    int num_threads,
                    const std::vector<std::map<std::string, std::string>>& customer_data,
                    const std::vector<std::map<std::string, std::string>>& orders_data,
                    const std::vector<std::map<std::string, std::string>>& lineitem_data,
                    const std::vector<std::map<std::string, std::string>>& supplier_data,
                    const std::vector<std::map<std::string, std::string>>& nation_data,
                    const std::vector<std::map<std::string, std::string>>& region_data,
                    std::map<std::string, double>& results) {

    std::string targetRegionKey;
    for (const auto& row : region_data) {
        if (row.at("r_name") == r_name) {
            targetRegionKey = row.at("r_regionkey");
            break;
        }
    }
    if (targetRegionKey.empty()) {
        std::cerr << "Region not found: " << r_name << std::endl;
        return false;
    }

    std::unordered_map<std::string, std::string> nationKeyToName;
    for (const auto& row : nation_data) {
        if (row.at("n_regionkey") == targetRegionKey) {
            nationKeyToName[row.at("n_nationkey")] = row.at("n_name");
        }
    }

    std::unordered_map<std::string, std::string> custKeyToNationKey;
    for (const auto& row : customer_data) {
        custKeyToNationKey[row.at("c_custkey")] = row.at("c_nationkey");
    }

    std::unordered_map<std::string, std::string> suppKeyToNationKey;
    for (const auto& row : supplier_data) {
        suppKeyToNationKey[row.at("s_suppkey")] = row.at("s_nationkey");
    }

    std::unordered_map<std::string, std::string> eligibleOrderToCust;
    for (const auto& row : orders_data) {
        const std::string& orderdate = row.at("o_orderdate");
        if (orderdate >= start_date && orderdate < end_date) {   // string comparison works for ISO dates!
            const std::string& custkey = row.at("o_custkey");
            auto it = custKeyToNationKey.find(custkey);
            if (it != custKeyToNationKey.end() && nationKeyToName.count(it->second)) {
                eligibleOrderToCust[row.at("o_orderkey")] = custkey;
            }
        }
    }

    std::vector<std::map<std::string, double>> partialResults(num_threads);

    auto worker = [&](int threadIdx, size_t startIdx, size_t endIdx) {
        std::map<std::string, double>& local = partialResults[threadIdx];
        for (size_t i = startIdx; i < endIdx; i++) {
            const auto& li = lineitem_data[i];

            auto orderIt = eligibleOrderToCust.find(li.at("l_orderkey"));
            if (orderIt == eligibleOrderToCust.end()) continue;   // order not eligible, skip

            const std::string& custNationKey = custKeyToNationKey.at(orderIt->second);

            auto suppIt = suppKeyToNationKey.find(li.at("l_suppkey"));
            if (suppIt == suppKeyToNationKey.end()) continue;
            if (suppIt->second != custNationKey) continue;        // customer & supplier must match nation

            double price = std::stod(li.at("l_extendedprice"));
            double discount = std::stod(li.at("l_discount"));
            local[nationKeyToName.at(custNationKey)] += price * (1.0 - discount);
        }
    };

    size_t total = lineitem_data.size();
    size_t chunkSize = (total + num_threads - 1) / num_threads;  // ceiling division

    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; t++) {
        size_t startIdx = t * chunkSize;
        size_t endIdx = std::min(startIdx + chunkSize, total);
        if (startIdx >= endIdx) continue;
        threads.emplace_back(worker, t, startIdx, endIdx);
    }
    for (auto& th : threads) th.join();

    for (const auto& partial : partialResults) {
        for (const auto& kv : partial) {
            results[kv.first] += kv.second;
        }
    }

    return true;
}

bool readLineitemSelective(const std::string& filepath,
                            std::vector<std::map<std::string, std::string>>& outData) {
    static const std::vector<std::string> allCols = {
        "l_orderkey","l_partkey","l_suppkey","l_linenumber","l_quantity",
        "l_extendedprice","l_discount","l_tax","l_returnflag","l_linestatus",
        "l_shipdate","l_commitdate","l_receiptdate","l_shipinstruct","l_shipmode","l_comment"
    };
    std::ifstream file(filepath);
    if (!file.is_open()) {
        std::cerr << "Failed to open file: " << filepath << std::endl;
        return false;
    }
    std::string line;
    while (std::getline(file, line)) {
        std::map<std::string, std::string> row;
        std::stringstream ss(line);
        std::string field;
        size_t colIndex = 0;
        while (std::getline(ss, field, '|') && colIndex < allCols.size()) {
            const std::string& colName = allCols[colIndex];
            // Only keep the 4 fields executeQuery5 actually uses -- saves significant memory
            if (colName == "l_orderkey" || colName == "l_suppkey" ||
                colName == "l_extendedprice" || colName == "l_discount") {
                row[colName] = field;
            }
            colIndex++;
        }
        outData.push_back(row);
    }
    file.close();
    return true;
}


bool outputResults(const std::string& result_path, const std::map<std::string, double>& results) {
    std::ofstream outFile(result_path + "/query5_results.txt");
    if (!outFile.is_open()) {
        std::cerr << "Failed to open output file at: " << result_path << std::endl;
        return false;
    }
    std::vector<std::pair<std::string, double>> sorted(results.begin(), results.end());
    std::sort(sorted.begin(), sorted.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });
    outFile << "n_name\trevenue\n";
    for (const auto& [nationName, revenue] : sorted) {
        outFile << nationName << "\t" << revenue << "\n";
    }
    outFile.close();
    std::cout << "Results written to: " << result_path + "/query5_results.txt" << std::endl;
    return true;
}