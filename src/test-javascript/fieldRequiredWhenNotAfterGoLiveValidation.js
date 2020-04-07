// we add a custom jquery validation method
jQuery.validator.addMethod('testrequiredwhennotaftergolivevalidation', function (value, element, params) {
    // is after go live?
    var isAfterGoLive = $("#IsAfterGoLive").val() == "True";

    if (!isAfterGoLive) {
        var tagName = $(element).get(0).tagName;
        var classNames = $(element).attr("class").split(" ");

        return value != null && ((tagName == "SELECT" && value > 0) || (tagName == "INPUT" && value != "")); 
    }
    return true;
});

// and an unobtrusive adapter
jQuery.validator.unobtrusive.adapters.add('requiredwhennotaftergolivevalidation', ['', ''], function (options) {
    options.rules['testrequiredwhennotaftergolivevalidation'] = true;
    options.messages['testrequiredwhennotaftergolivevalidation'] = options.message;
});