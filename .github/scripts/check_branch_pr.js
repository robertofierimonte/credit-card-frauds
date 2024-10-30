module.exports = async ({github, context, core}) => {
    const resp = await github.request('GET /repos/{owner}/{repo}/pulls', {
        owner: context.repo.owner,
        repo: context.repo.repo,
    });
    if (resp.data) {
        const pullRequestsWithBranch = resp.data.filter(it => it.head.ref == process.env.BRANCH_NAME)
        return pullRequestsWithBranch.length >= 1
    }
    return false
}
