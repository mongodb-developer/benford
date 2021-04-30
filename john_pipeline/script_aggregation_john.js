let firstDigitInConfirmed = {
  "$cond": [{"$gt": ["$confirmed_daily", 0]}, {
    "$toInt": {
      "$let": {
        "vars": {
          "m": {
            "$regexFind": {
              "input": {"$toString": "$confirmed_daily"},
              "regex": "[1-9]"
            }
          }
        },
        "in": "$$m.match"
      }
    }
  }, null]
};

let firstDigitInDeaths = {
  "$cond": [{"$gt": ["$deaths_daily", 0]}, {
    "$toInt": {
      "$let": {
        "vars": {
          "m": {
            "$regexFind": {
              "input": {"$toString": "$deaths_daily"},
              "regex": "[1-9]"
            }
          }
        },
        "in": "$$m.match"
      }
    }
  }, null]
};

let groupByCountry = {
  "$group": {
    "_id": "$country",
    "confirmed": {"$push": firstDigitInConfirmed},
    "deaths": {"$push": firstDigitInDeaths}
  }
};

let noNulls = {
  "$set": {
    "deaths": {
      "$filter": {
        "input": "$deaths",
        "cond": {"$gt": ["$$this", 0]}
      }
    },
    "confirmed": {
      "$filter": {
        "input": "$confirmed",
        "cond": {"$gt": ["$$this", 0]}
      }
    }
  }
};

let sizesNotZero = {"$match": {"$expr": {"$and": [{"$gt": [{"$size": "$confirmed"}, 0]}, {"$gt": [{"$size": "$deaths"}, 0]}]}}};

let final = {
  "$project": {
    "confirmed_size": {"$size": "$confirmed"},
    "deaths_size": {"$size": "$deaths"},
    "benford": {
      "$map": {
        "input": {"$range": [1, 10]},
        "as": "digit",
        "in": {
          "digit": "$$digit",
          "deaths": {
            "$divide": [{
              "$multiply": [100, {
                "$size": {
                  "$filter": {
                    "input": "$deaths",
                    "cond": {"$eq": ["$$this", "$$digit"]}
                  }
                }
              }]
            }, {"$size": "$deaths"}]
          },
          "confirmed": {
            "$divide": [{
              "$multiply": [100, {
                "$size": {
                  "$filter": {
                    "input": "$confirmed",
                    "cond": {"$eq": ["$$this", "$$digit"]}
                  }
                }
              }]
            }, {"$size": "$confirmed"}]
          },
          "benford": {"$arrayElemAt": [[0, 30.1, 17.6, 12.5, 9.7, 7.9, 6.7, 5.8, 5.1, 4.6], "$$digit"]}
        }
      }
    }
  }
};

let pipeline = [groupByCountry, noNulls, sizesNotZero, final];

let cursor = db.countries_summary.aggregate(pipeline);

printjson(cursor.next());
