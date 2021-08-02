(function() {


    angular.module("DKV.Dashboard.Controllers", ['DKV.Dashboard.Services','chart.js', 'dataGrid', 'pagination', 'ngCookies' ,'ngclipboard','mgcrea.ngStrap','ngSanitize'])

        .config(['ChartJsProvider', function (ChartJsProvider) {
            ChartJsProvider.setOptions({
                responsive: false
            });
            ChartJsProvider.setOptions('line', {
                showLines: true,
                animation: {
                	duration:0
                },
                legend: {
                    display: true,
                    position: 'bottom',
                    labels:{
                        boxWidth:20
                    }
                },
                scales: {
                    yAxes: [{
                        ticks: {
                            beginAtZero:true
                        },
                        gridLines: {
                            display:false
                        }
                    }],
                    xAxes: [{
                        type: 'time',
                        ticks: {
                            maxRotation: 0,
                            minRotation: 0,
                            autoSkip: true,
                            maxTickLimit: 3
                        },
                        gridLines: {
                            display:false
                        }
                    }]
                },
                tooltips: {
                    intersect:false,
                    callbacks : {
                        title: function(tooltipItems, data) {
                            return tooltipItems[0].xLabel.toLocaleTimeString();
                        }
                    }
                },
                elements: {
                        line: {
                            fill: false,
                            borderWidth: 1
                        },
                        point: {
                            radius: 1
                        }

                    }
            });
        }])


        .controller("HomeCtrl", [ '$location', '$scope', '$rootScope','$timeout','$sce','DKVService',
            function($location, $scope, $rootScope ,$timeout, $sce, DKVService) {

            }
        ])
}());
