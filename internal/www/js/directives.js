(function() {

    angular.module("DKV.Dashboard.Directives", ["DKV.Dashboard.Controllers"])
        .directive("headerNav", function() {
            return {
                restrict: "E",
                templateUrl: "header-nav.html",
                controller: function($scope, $rootScope, $location, $sce) {
                    $scope.config = CONFIG;
                }
            };
        })
}());
