(function() {
    angular.module("DKV.Dashboard.Services", [])

        .factory("DKVService", [ '$http','$sce', function($http,$sce) {
            return {
                GetStreamUrl: function(addressLink , env) {
                    return $sce.trustAsResourceUrl("/metrics/stream")
                },
            }
        }])
}());

