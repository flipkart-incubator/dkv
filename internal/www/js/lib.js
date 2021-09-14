var throuputParser = function (d) {
    var sizes = ['', 'K', 'M', 'B', 'T'];
    if (d < 1) return d.toFixed(1);
    var i = Math.floor(Math.log(d) / Math.log(1000));
    var base = d / Math.pow(1000, i);
    if ( Math.round(base) === base ){
        return base + ' ' + sizes[i]
    }
    return base.toFixed(1) + ' ' + sizes[i];
};

var latencyParser = function (d) {
    d = d * 1000000
    var sizes = ['µs', 'ms', 's'];
    var i = Math.floor(Math.log(d) / Math.log(1000));
    var base = d / Math.pow(1000, i);
    if ( Math.round(base) === base ){
        return base + ' ' + sizes[i]
    }
    return base.toFixed(1) + ' ' + sizes[i];
};


var percentageParser = function (d) {
    if (Math.round(d) === d) return Math.round(d)+" %";
    return d.toFixed(2)+" %";
}

function getEmptyArray(size){
    var data = [];
    for (i = 0; i < size; i++) {
        data.push([]);
    }
    return data;
}
var colorGreen = {backgroundColor: '#62a043', borderColor: '#62a043', hoverBackgroundColor: '#62a043', hoverBorderColor: '#62a043'};
var colorOrange = {backgroundColor: '#dc923f', borderColor: '#dc923f', hoverBackgroundColor: '#dc923f', hoverBorderColor: '#dc923f'};
var colorBlue = {backgroundColor: '#0a9bdc', borderColor: '#0a9bdc', hoverBackgroundColor: '#0a9bdc', hoverBorderColor: '#0a9bdc'};
var colorRed = {backgroundColor: '#bc4040', borderColor: '#bc4040', hoverBackgroundColor: '#bc4040', hoverBorderColor: '#bc4040'};
var colorYellow = {backgroundColor: '#ffe50c', borderColor: '#ffe50c', hoverBackgroundColor: '#ffe50c', hoverBorderColor: '#ffe50c'};
var colorViolet = {backgroundColor: '#9263ff', borderColor: '#9263ff', hoverBackgroundColor: '#9263ff', hoverBorderColor: '#9263ff'};
var colorDeepBlue = {backgroundColor: '#0001ff', borderColor: '#0001ff', hoverBackgroundColor: '#0001ff', hoverBorderColor: '#0001ff'};
