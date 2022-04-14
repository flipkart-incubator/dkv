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
            /* discover master nodes from here*/
                $scope.masters = {
                    "m1":"http://127.0.0.1:8081",
                    "m2":"http://127.0.0.1:8082",
                    "m3":"http://127.0.0.1:8083",
                }

                $scope.througputParser = througputParser
                $scope.latencyParser = latencyParser
                $scope.getMapValSum = getMapValSum

                $scope.ops1 = ["getLin","getSeq","mgetLin","mgetSeq",]
                $scope.ops2 = ["put","mput","cas","del"]

                $scope.source = new EventSource(DKVService.GetClusterData($scope.masters.m1));
                $scope.source.addEventListener('message', function(e) {
                    data = JSON.parse(e.data)
                    $scope.stat = data["global"]
                    delete data["global"]
                    $scope.stats = data
                    /* initialize after the first population */
                    if ( $scope.series === undefined ) {
                        initMetric()
                    }

                    console.log(new Date($scope.stat.ts))
                    console.log(new Date())
                    $scope.tsEvent.push(new Date($scope.stat.ts));

                    for ( i = 0 ; i < $scope.series.length; i++ ) {
                        $scope.ts.rrate.data[i].push(getMapValSum($scope.stats[$scope.series[i]]["dkv_req_count"]))
                        $scope.ts.error.data[i].push(getMapValSum($scope.stats[$scope.series[i]]["dkv_req_count"]) / 100)
                        avgLatency = getAvgLatency($scope.stats[$scope.series[i]]["dkv_latency"])
                        $scope.ts.p50.data[i].push(avgLatency.p50)
                        $scope.ts.p99.data[i].push(avgLatency.p99)
                    }

                    if ( $scope.tsEvent.length > 60 ) {
                        $scope.tsEvent.splice(0,1);
                        for ( i = 0 ; i < $scope.series.length; i++ ) {
                            $scope.ts.rrate.data[i].splice(0,1);
                            $scope.ts.error.data[i].splice(0,1);
                            $scope.ts.p50.data[i].splice(0,1);
                            $scope.ts.p99.data[i].splice(0,1);
                        }
                    }

                });

                function createStat(parser,titleText) {
                    var stat = { series : $scope.series, colors: $scope.colors, label: $scope.tsEvent, data : getEmptyArray($scope.series.length),
                        options: {
                            responsive: true,
                            maintainAspectRatio: false,
                            scales: {
                                yAxes: [{
                                    ticks: {
                                        callback: function (value, index, values) {
                                            return parser(value)
                                        }
                                    },
                                }]
                            },
                            title: {
                                display: true, text: titleText,
                                fontSize: 13, fontColor: "#000000"
                            },
                            legend: {
                                display: false
                            },
                            elements: {
                                line: {
                                    fill: false,
                                    borderWidth: 1.7
                                },
                                point: {
                                    radius: 0
                                }

                            }
                        }
                    };
                    return stat
                }
                function initMetric() {
                    $scope.ts = {}
                    $scope.tsEvent = []
                    $scope.series = Object.keys($scope.stats)
                    $scope.colors = [colorGreen,colorOrange,colorBlue,colorRed,colorViolet,colorDeepBlue]

                    $scope.ts.rrate = createStat(througputParser,"request rate")
                    $scope.ts.error = createStat(percentageParser,"error rate")
                    $scope.ts.p50 = createStat(latencyParser,"50th percentile")
                    $scope.ts.p99 = createStat(latencyParser,"99th percentile")
                }

                window.setInterval(function(){
                    $scope.$apply(function () {
                    });
                }, 1000);
            }
        ])
}());
