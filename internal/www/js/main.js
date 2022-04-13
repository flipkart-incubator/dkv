(function() {

    angular.module("DKV.Dashboard",['ngRoute', 'DKV.Dashboard.Services', 'DKV.Dashboard.Controllers', 'DKV.Dashboard.Directives'])

    /** routes */
        .config( function($routeProvider,$locationProvider) {
            for (id in CONFIG.router) {
                $routeProvider.when(CONFIG.router[id].path, {
                    templateUrl: CONFIG.router[id].templateUrl,
                    controller: CONFIG.router[id].controller
                });
            }
            $routeProvider.otherwise({
                redirectTo: "/admin/"
            });
            $locationProvider.html5Mode(true);
        })

        .run(['$rootScope', '$location', '$anchorScroll', function($rootScope, $location, $anchorScroll) {
            $rootScope.$on('$routeChangeSuccess', function(newRoute, oldRoute) {
                if ($location.hash()) $anchorScroll();
            });
        }]);

}());
