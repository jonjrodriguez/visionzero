<template>
  <div>
    <nav class="navbar navbar-fixed-top navbar-dark bg-inverse mb-2">
        <router-link to="/" class="navbar-brand">Vision Zero</router-link>
        <ul class="nav navbar-nav">
            <li class="nav-item">
                <router-link to="/data" class="nav-item nav-link">Data Sources</router-link>
                <router-link to="/charts" class="nav-item nav-link">Charts</router-link>
            </li>
        </ul>
    </nav>

    <div class="container-fluid">
        <div class="row">
            <div class="col-sm-12">
                <transition name="fade" mode="out-in">
                    <router-view class="view"></router-view>
                </transition>
            </div>
        </div>
    </div>
  </div>
</template>

<script>
    export default {
        name: 'App'
    }
</script>

<style>
    .fade-enter-active, .fade-leave-active {
        transition: opacity .3s ease;
    }
    .fade-enter, .fade-leave-active {
        opacity: 0;
    }
</style>