<script>
	import '@evidence-dev/tailwind/fonts.css';
	import '../app.css';
	import { EvidenceDefaultLayout } from '@evidence-dev/core-components';
	export let data;
</script>

<EvidenceDefaultLayout {data} logo="/new_logo.svg" githubRepo="https://github.com/liamtabib/geostreamline">
	<slot slot="content" />
</EvidenceDefaultLayout>