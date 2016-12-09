import Vue from "vue";
import Router from "vue-router";

Vue.use(Router);

import VisionZero from './views/VisionZero.vue';
import DataSources from './views/DataSources.vue';
import Charts from './views/Charts.vue';
import NotFound from './views/NotFound.vue';

export default new Router({
    mode: "history",
    linkActiveClass: "active",
    scrollBehavior: (to, from, savedPosition) => savedPosition || { x: 0, y: 0 },
    routes: [
        { path: '/', component: VisionZero },
        { path: '/data', component: DataSources },
        { path: '/charts', component: Charts },
        { path: '*', component: NotFound }
    ]
});