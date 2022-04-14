(function() {

    angular.module("DKV.Dashboard.Directives", ["DKV.Dashboard.Controllers"])
        .directive("headerNav", function() {
            return {
                restrict: "E",
                templateUrl: "/admin/header-nav.html",
                controller: function($scope, $rootScope, $location, $sce) {
                    $scope.config = CONFIG;
                }
            };
        })
}());
