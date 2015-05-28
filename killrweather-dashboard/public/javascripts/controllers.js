'use strict';

// controllers
var killrControllers = angular.module('killrControllers', []);

killrControllers.controller('WeatherStationListCtrl', ['$scope', '$http', 'ngTableParams', '$filter',
    function ($scope, $http, ngTableParams, $filter) {
        $http.get('/station').success(function (data) {
            $scope.tableParams = new ngTableParams({
                page: 1,
                count: 10
            }, {
                total: data.length,
                getData: function ($defer, params) {
                    var orderedData = params.filter() ?
                        $filter('filter')(data, params.filter()) :
                        data;

                    $scope.stations = orderedData.slice((params.page() - 1) * params.count(), params.page() * params.count());
                    params.total(orderedData.length);
                    $defer.resolve($scope.stations);
                }
            });
        });
    }
]);

killrControllers.controller('WeatherStationViewCtrl', ['$scope', '$http', '$routeParams', '$websocket',
    function ($scope, $http, $routeParams, $websocket) {
        this.tab = 0;
        this.selectTab = function(setTab) {
            this.tab = setTab;
        };
        this.isSelected = function(checkTab) {
            return this.tab == checkTab;
        };

        $scope.loadSpec = {};
        $scope.datapoints = [];
        $scope.datacolumns = [{"id": "precipitation", "type": "line", "name": "Daily Precipitation"}];
        $scope.datax = {"id": "date"};

        $http.get('/station/' + $routeParams.id).success(function (data) {
            $scope.station = data;
            $scope.datapoints = data.dailyPrecipitation;
            console.info("Setting data points to " + JSON.stringify($scope.datapoints));
        });

        var ws = $websocket.$new('ws://localhost:9000/stream/station/' + $routeParams.id);

        ws.$on('$open', function () {
            console.log('Websocket connection established');
        });

        ws.$on('weatherUpdate', function (data) {
            console.log('Received new weather data');
            $scope.$apply(function() {
                $scope.datapoints = data.dailyPrecipitation;
                console.info("Setting data points to " + JSON.stringify($scope.datapoints));
            });
        });

        ws.$on('$close', function () {
            console.log('Websocket closed by server');
        });


    }
]);

killrControllers.controller('LoadGenerationCtrl', ['$scope', '$http', '$routeParams',
    function($scope, $http, $routeParams) {
        var past = moment().add(-30, 'days');
        $scope.loadSpec = { startDate: past, endDate: moment() };
        $scope.submissionSuccess = false;
        $scope.submissionFailure = false;

        $scope.sendLoad = function() {
            $scope.submissionSuccess = false;
            $scope.submissionFailure = false;
            var data = {id: $routeParams.id, loadSpec: $scope.loadSpec};
            console.info("To send: " + JSON.stringify(data));
            $http.post("/load", data).
                success(function(data, status, headers, config) {
                    $scope.submissionSuccess = true;
                    $scope.loadSpec = {};
                }).
                error(function(data, status, headers, config) {
                    $scope.submissionFailure = true;
                });
        }
    }
]);


