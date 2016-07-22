var params = {
  eventToMeasure: "App Open", //This is where you'd specify event/data type you want to track
  first_date_of_valid_data: '2015-10-01' //Please make this the first valid week of your data. Often your first week of MP data is janky due to testing etc.
  from_date: '2016-05-01', //the date you want to measure from
  to_date: '2016-07-21' //the date you want to measure to
}

function main() {
  return join(
    Events({
      from_date: params.first_date_of_valid_data,
      to_date:  params.to_date,
      event_selectors: [{event: params.eventToMeasure}] 
    }),
    People()
  )
  .filter(function (tuple) { //if you want to add filters to specify only a specific user type those would go here
    return tuple.event //&& tuple.user (simply tag on the appropriate attributes to this .user)
  })
  .groupByUser(function(state, allEventsArray) {
      state = state || {
        toInclude: true, //keep track of whether this user should be included or not
        cohortWeek: null, //*********  Enter Buckets Here. Must follow #_String Pattern ***********/
        "04 Week LTV": 0, //*********  Must Also Update Logic Cascade (in for loop below) ******/
        "08 Week LTV": 0, //********* (If You change/Add more Buckets) ***********/
        "12 Week LTV": 0,
        "16 Week LTV": 0,
        "20 Week LTV": 0,
        "24 Week LTV": 0,
      };
      if (!state.cohortWeek) {
        var cohortStartDate = getPreviousSunday(allEventsArray[0].event.time); //MP groupByUser has events array chronologically ordered
        state.cohortWeek = cohortStartDate;
        if (new Date(allEventsArray[0].event.time) < new Date (getPreviousSunday(params.from_date))) {
          state.toInclude = false; //set this to false so we can filter out later
        } 
      }
      if (!state.toInclude) {
        return state;
      }
      for (var i = 0; i < allEventsArray.length; i++) { //loop through all events
        var event = allEventsArray[i]; //sanity check
        var eventWeek = getPreviousSunday(event.event.time);
        var timeBetween = calculateWeeksBetween(eventWeek, state.cohortWeek);
        if (i > 0) {
          state.test = timeBetween
        }
        //Simply add this event to the appropriate count bucket (we'll aggregate later on)
        /************************ Also update Bucketing here  ******************/
        if (timeBetween <= 4) { //not <= 4 as timeBetween < 4 means 4 weeks have not yet passed
          state["04 Week LTV"]++;
        } else if (timeBetween <= 8) { 
          state["08 Week LTV"]++; 
        } else if (timeBetween <= 12) { 
          state["12 Week LTV"]++;
        } else if (timeBetween <= 16) { 
          state["16 Week LTV"]++
        } else if (timeBetween <= 20) { 
          state["20 Week LTV"]++;
        } else if (timeBetween <= 24) { 
          state["24 Week LTV"]++;
        } /*********************** Update above cascade *******************/
      }
      return state
  })
  .filter(function(row){ //remove cohorts that should not be included
        return row && row.value.toInclude
  })
  .groupBy(
    ["value.cohortWeek"],
    function (previousResults, unProcessedEvents){
      var resultObject = {cohortSize: 0};
      for (var eventKey in unProcessedEvents) { //go through events
        var row = unProcessedEvents[eventKey].value; //row of data from single user, just get values as don't need userID
        resultObject.cohortSize++ //one person for this cohort, so increment size
        for (var column in row) { //get all columns (nWeek LTV options)
          if (column == "cohortWeek" || column == "test" || column == "toInclude") {//skip cohort week as that is now the key
            continue;
          }
          if (resultObject[column]) { //column already exists for this (we already have a value for '4 week ltv')
            resultObject[column] += row[column] //so increment the value in this column by the value for this user 
          } else { //first time column appears, set it equal to value of this user
            resultObject[column] = row[column];
          }
        }
      }
      
      for (var result in previousResults) { //previous Results is an array of returned values from processed events
        for (var key in previousResults[result]) {
          if (resultObject[key]) {
            resultObject[key] += previousResults[result][key]; //increment by amount stored in previous bit
          } else {
            resultObject[key] = previousResults[result][key]; //make sure this works
          }
          
        }
      }
      return resultObject
      //need to turn into averages!!
    })
    .map(function (row) { //convert each week LTV value from aggregate to average
      var newRow = {};    //also nullify 4Week LTV if 4 weeks have yet to pass
      for (var key in row) {
        if (key == "key") {
          newRow['Cohort Week'] = row.key[0]; //MP stores the row as key:val where key = groupBy value from previous groupby, and there can be multiple so we want index 0 of that. thus row.key.0!
        } else {
          var valuesObj = row[key];
          for (var valueKey in valuesObj) {
            if (valueKey == "cohortSize") {
              newRow[valueKey] = valuesObj[valueKey];
            } else if (valueKey == "04 Week LTV") { //check to make sure at least 4 weeks have passed for cohort
              var cohortDate = row.key[0]; //Same as above reference
              var lastDate = getPreviousSunday(params.to_date); //remember that to_date param we set ;-)!!!
              var weeksBetween = calculateWeeksBetween(lastDate, cohortDate);
              if (weeksBetween < 4) {
                newRow["04 Week LTV"] = null;
              } else {
                newRow["04 Week LTV"] = Math.round(valuesObj[valueKey] / row[key]['cohortSize']); //want average 
              }
            } 
            else {
              var accumulatedValue = calculateTotalEventsForKey(valueKey, valuesObj);
              if (accumulatedValue) { //meaning total events are not null
                newRow[valueKey] = Math.round(accumulatedValue / row[key]['cohortSize']); //want average 
              } else {
                newRow[valueKey] = null; //set 0 values to null for easier graphing
              }
            }
          }
        }
      }
      return newRow;
    })
}

/********* calculates the closest, next, sunday for a given event *********/
function getPreviousSunday(rawTime) {
  var date = (new Date(rawTime))
  date.setDate(date.getDate()  - date.getDay()); 
  return date.toISOString().split('T')[0];
}

/********* calculates the weeks between two dates. Likely being passed in as a string *********/
function calculateWeeksBetween(eventWeek, cohortWeek) {
  var weeksBetween = Math.round((new Date(eventWeek)-new Date(cohortWeek)) / 604800000); //the number of ms in week
  if (weeksBetween <= 1) {
    weeksBetween = 1;
  }
  return weeksBetween;
}

/*** Given the current week bucket String, and OBJ containing values of all weeks,
* adds previous weeks to current bucket **********/
function calculateTotalEventsForKey(currentWeek, valuesObj) {
  var total = valuesObj[currentWeek];
  var number = currentWeek.split(' ')[0]; //get the numeric value of the week.
  for (var key in valuesObj) {
    if (key == "cohortSize") {
      continue;
    }
    if (number > key.split(' ')[0]) {
      if (total !== 0) { //will be equal to 0 if no events occured in that time frame
        total+= valuesObj[key];
      }
    }
  }
  return total;
}
