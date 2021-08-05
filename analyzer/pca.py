from sklearn.decomposition import PCA


data = [[1, 0, 2, 0],
        [0, 4, 0, 3],
        [4, 0, 8, 0],
        [0, 1, 0, 0],
        [2, 3, 4, 2],
        [3, 2, 6, 1],
        [1, 1, 2, 0],
        [4, 2, 8, 1]]

pca = PCA(n_components=2)
principal_components = pca.fit_transform(data)
original = pca.inverse_transform(principal_components)
