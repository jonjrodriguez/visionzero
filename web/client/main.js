import Vue from 'vue';
import { client } from 'vue-tables-2';
import App from './App.vue';
import router from './router';
import * as filters from './filters'

Vue.use(client);

// register global utility filters.
Object.keys(filters).forEach(key => {
  Vue.filter(key, filters[key])
})

new Vue({
  el: '#app',
  router,
  render: h => h(App)
});