let groupBy = {
  "$group": {
    "_id": "$country",
    "confirmed": {
      "$push": {
        "$substr": [{
          "$toString": "$confirmed_daily"
        }, 0, 1]
      }
    },
    "deaths": {
      "$push": {
        "$substr": [{
          "$toString": "$deaths_daily"
        }, 0, 1]
      }
    }
  }
};

let createConfirmedAndDeathsArrays = {
  "$addFields": {
    "confirmed": {
      "$filter": {
        "input": "$confirmed",
        "as": "elem",
        "cond": {
          "$and": [{
            "$ne": ["$$elem", "0"]
          }, {
            "$ne": ["$$elem", "-"]
          }]
        }
      }
    },
    "deaths": {
      "$filter": {
        "input": "$deaths",
        "as": "elem",
        "cond": {
          "$and": [{
            "$ne": ["$$elem", "0"]
          }, {
            "$ne": ["$$elem", "-"]
          }]
        }
      }
    }
  }
};

let addArraySizes = {
  "$addFields": {
    "confirmed_size": {
      "$size": "$confirmed"
    },
    "deaths_size": {
      "$size": "$deaths"
    }
  }
};

let removeCountriesWithoutConfirmedCasesAndDeaths = {
  "$match": {
    "confirmed_size": {
      "$gt": 0
    },
    "deaths_size": {
      "$gt": 0
    }
  }
};

function calculatePercentage(inputArray, digit, sizeArray) {
  return {
    "$round": [{
      "$divide": [{
        "$multiply": [100, {
          "$size": {
            "$filter": {
              "input": inputArray,
              "as": "elem",
              "cond": {
                "$eq": ["$$elem", digit]
              }
            }
          }
        }]
      }, sizeArray]
    }, 1]
  }
}

function calculatePercentageConfirmed(digit) {
  return calculatePercentage("$confirmed", digit, "$confirmed_size");
}

function calculatePercentageDeaths(digit) {
  return calculatePercentage("$deaths", digit, "$deaths_size");
}

let calculateBenfordPercentagesConfirmedAndDeaths = {
  "$addFields": {
    "benford": [{
      "digit": 1,
      "confirmed": calculatePercentageConfirmed("1"),
      "deaths": calculatePercentageDeaths("1")
    }, {
      "digit": 2,
      "confirmed": calculatePercentageConfirmed("2"),
      "deaths": calculatePercentageDeaths("2")
    }, {
      "digit": 3,
      "confirmed": calculatePercentageConfirmed("3"),
      "deaths": calculatePercentageDeaths("3")
    }, {
      "digit": 4,
      "confirmed": calculatePercentageConfirmed("4"),
      "deaths": calculatePercentageDeaths("4")
    }, {
      "digit": 5,
      "confirmed": calculatePercentageConfirmed("5"),
      "deaths": calculatePercentageDeaths("5")
    }, {
      "digit": 6,
      "confirmed": calculatePercentageConfirmed("6"),
      "deaths": calculatePercentageDeaths("6")
    }, {
      "digit": 7,
      "confirmed": calculatePercentageConfirmed("7"),
      "deaths": calculatePercentageDeaths("7")
    }, {
      "digit": 8,
      "confirmed": calculatePercentageConfirmed("8"),
      "deaths": calculatePercentageDeaths("8")
    }, {
      "digit": 9,
      "confirmed": calculatePercentageConfirmed("9"),
      "deaths": calculatePercentageDeaths("9")
    }]
  }
};

let unionBenfordTheoreticalValues = {
  "$unionWith": {
    "coll": "benford"
  }
};

let finalProjection = {
  "$project": {
    "country": "$_id",
    "_id": 0,
    "benford": 1,
    "confirmed_size": 1,
    "deaths_size": 1
  }
};

let pipeline = [groupBy,
                createConfirmedAndDeathsArrays,
                addArraySizes,
                removeCountriesWithoutConfirmedCasesAndDeaths,
                calculateBenfordPercentagesConfirmedAndDeaths,
                unionBenfordTheoreticalValues,
                finalProjection];

let cursor = db.countries_summary.aggregate(pipeline);

printjson(cursor.next());
