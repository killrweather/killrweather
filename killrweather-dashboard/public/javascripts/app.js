'use strict';

/* App Module */

var killrWeather = angular.module('killrWeather', [
    'ngWebsocket',
    'ngTable',
    'ngRoute',
    'killrControllers',
    'gridshore.c3js.chart',
    'ui.bootstrap.datetimepicker'
]);

killrWeather.config(['$routeProvider',
    function($routeProvider) {
        $routeProvider.
            when('/station/:id', {
                templateUrl: 'station-view.html',
                controller: 'WeatherStationViewCtrl'
            }).
            when('/station', {
                templateUrl: 'station-list.html',
                controller: 'WeatherStationListCtrl'
            }).
            otherwise({
                 redirectTo: '/station'
            });
    }]);