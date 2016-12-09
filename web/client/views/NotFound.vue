<template>
    <div>
        <TitleHeader title="404" />

        <div class="mt-3 mb-2">
            <h3 class="text-xs-center mb-0">
                We couldn't find the page...
            </h3>
        </div>

        <div class="text-xs-center offset-md-3 col-md-6">
            <p>Sorry, but the page you are looking for was either not found or does not exist. Try refreshing the page or click the link below to go back to the Homepage.</p>
            <p><a class="btn" href="/">Home</a></p>
        </div>
    </div>
</template>

<script>
    import TitleHeader from "../components/Header.vue";

    export default {
        name: "NotFound",

        components: { TitleHeader }
    }
</script>