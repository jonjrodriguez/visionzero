<template>
    <div class="heading mb-2 mt-3 pt-2">
        <h1 class="display-4 text-xs-center mb-0">
            {{ title }}
        </h1>
    </div>
</template>

<script>
    export default {
        name: "Header",

        props: ["title"]
    }
</script>