(function() {
    angular.module("DKV.Dashboard.Services", [])

        .factory("DKVService", [ '$http','$sce', function($http,$sce) {
            return {
                GetClusterData: function(endpoint) {
                    return $sce.trustAsResourceUrl(endpoint+"/metrics/cluster")
                },
            }
        }])
}());

