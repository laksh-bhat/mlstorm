/*
package classifier.unsupervisedlearning.image_denoise;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class MRFImageProcessor {
    private double eta;
    private double beta;
    private double omega;
    private int maxColor;
    private int minColor;
    private double colorsAvg;
    private int numK;
    private int iterations;
    private boolean useSecondLevel;

    public MRFImageProcessor(
            double eta,
            double beta,
            double omega,
            int num_iterations,
            boolean use_second_level,
            int num_k) {
        this.eta = eta;
        this.beta = beta;
        this.numK = num_k;
        this.omega = omega;
        this.iterations = num_iterations;
        this.useSecondLevel = use_second_level;

    }

    public int[][] denoisifyImage(int[][] observedImage) {
        int count = ImageUtils.countColors(observedImage, false);
        maxColor = ImageUtils.maxColor(observedImage);

        switch (count) {
            case 1:
                return observedImage;

            case 2:
                return denoisifyBlackAndWhiteImage(observedImage, useSecondLevel);

            default:
                return denoisifyGreyScaleImage(observedImage, useSecondLevel);
        }
    }

    private int[][] denoisifyGreyScaleImage(int[][] observedImage, boolean useSecondLevel) {
        if (useSecondLevel) {
            return denoisifyGSImageWith2HiddenLevels(observedImage);
        } else {
            return denoisifyGreyScaleImage(observedImage);
        }
    }

    private int[][] denoisifyBlackAndWhiteImage(int[][] observedImage, boolean useSecondLevel) {
        minColor = 0;
        colorsAvg = maxColor / 2;

        if (useSecondLevel) {
            return denoisifyBWImageWith2HiddenLevels(observedImage);
        } else {
            return denoisifyBlackAndWhiteImage(observedImage);
        }
    }

    private void initializeHiddenNodesIn2ndLevel(
            int[][] zLayer,
            int[][] xLayer,
            int numColors) {
        int[][] zBlockCount = new int[zLayer.length][zLayer[0].length];
        for (int ii = 0; ii < xLayer.length; ii++) {
            for (int jj = 0; jj < xLayer[ii].length; jj++) {
                int m = (int) Math.floor(ii / numK);
                int n = (int) Math.floor(jj / numK);
                zLayer[m][n] += xLayer[ii][jj];
                zBlockCount[m][n]++;
            }
        }

        if (numColors == 2) {
            for (int ii = 0; ii < zLayer.length; ii++) {
                for (int jj = 0; jj < zLayer[ii].length; jj++) {
                    double avg = zLayer[ii][jj] / zBlockCount[ii][jj];
                    zLayer[ii][jj] = avg >= colorsAvg ? maxColor : minColor;
                }
            }
        } else {
            for (int ii = 0; ii < zLayer.length; ii++) {
                for (int jj = 0; jj < zLayer[ii].length; jj++) {
                    zLayer[ii][jj] /= zBlockCount[ii][jj];
                }
            }
        }
    }

    private boolean isConnected(int i, int j, int m, int n, int numK) {
        return (Math.floor(i / numK) == m) && (Math.floor(j / numK) == n);
    }

    private int[][] denoisifyGSImageWith2HiddenLevels(int[][] yLayer) {
        int[][] xLayer = new int[yLayer.length][yLayer[0].length];
        int[][] tempXLayer = new int[yLayer.length][yLayer[0].length];

        int row = (yLayer.length / numK == yLayer.length / numK + yLayer.length % numK) ? yLayer.length / numK :
                yLayer.length / numK + 1;
        int col = (yLayer[0].length / numK == yLayer[0].length / numK + yLayer[0].length % numK) ? yLayer[0
                ].length / numK : yLayer[0].length / numK + 1;

        int[][] zLayer = new int[row][col];
        int[][] tempZLayer = new int[row][col];

        HashMap<Integer, Integer> colorMap = ImageUtils.createColorMap(yLayer);

        copyLayer(xLayer, yLayer);
        initializeHiddenNodesIn2ndLevel(zLayer, xLayer, colorMap.size());

        while (iterations-- > 0) {
            for (int ii = 0; ii < xLayer.length; ii++) {
                for (int jj = 0; jj < xLayer[ii].length; jj++) {
                    int zRowIdx = ii / numK, zColIdx = jj / numK;

                    List<Integer> neighbors = getNeighbors(xLayer, ii, jj);
                    int finalColorValue = decideColorGreyScaleEx(yLayer[ii][jj],
                            zLayer[zRowIdx][zColIdx], neighbors);
                    tempXLayer[ii][jj] = finalColorValue;
                }
            }

            copyLayer(xLayer, tempXLayer);

            for (int ii = 0; ii < zLayer.length; ii++) {
                for (int jj = 0; jj < zLayer[ii].length; jj++) {
                    List<Integer> neighborsOfZ = getNeighborsOfZi(xLayer, ii, jj);
                    int finalColorValue = neighborsOfZ.size() > 0 ? decideColorForZPixelGS(neighborsOfZ) :
                            zLayer[ii][jj];
                    tempZLayer[ii][jj] = finalColorValue;
                }
            }


            copyLayer(zLayer, tempZLayer);
        }
        return xLayer;
    }

    private int[][] denoisifyBWImageWith2HiddenLevels(int[][] yLayer) {
        int[][] xLayer = new int[yLayer.length][yLayer[0].length];
        int[][] tempXLayer = new int[yLayer.length][yLayer[0].length];

        int row = (yLayer.length / numK == yLayer.length / numK + yLayer.length % numK) ? yLayer.length / numK :
                yLayer.length / numK + 1;
        int col = (yLayer[0].length / numK == yLayer[0].length / numK + yLayer[0].length % numK) ? yLayer[0
                ].length / numK : yLayer[0].length / numK + 1;

        int[][] zLayer = new int[row][col];
        int[][] tempZLayer = new int[row][col];

        HashMap<Integer, Integer> colorMap = ImageUtils.createColorMap(yLayer);

        copyLayer(xLayer, yLayer);
        initializeHiddenNodesIn2ndLevel(zLayer, xLayer, colorMap.size());

        while (iterations-- > 0) {
            for (int ii = 0; ii < xLayer.length; ii++) {
                for (int jj = 0; jj < xLayer[ii].length; jj++) {
                    int zRowIdx = ii / numK, zColIdx = jj / numK;

                    List<Integer> neighbors = getNeighbors(xLayer, ii, jj);
                    int chosenColor = decideColorBorWEx(yLayer[ii][jj], zLayer[zRowIdx][zColIdx],
                            neighbors);
                    tempXLayer[ii][jj] = chosenColor;
                }
            }
            copyLayer(xLayer, tempXLayer);

            for (int ii = 0; ii < zLayer.length; ii++) {
                for (int jj = 0; jj < zLayer[ii].length; jj++) {
                    List<Integer> neighborsOfZ = getNeighborsOfZi(xLayer, ii, jj);
                    int chosenColor = decideColorForZPixelBW(zLayer[ii][jj], neighborsOfZ);
                    tempZLayer[ii][jj] = chosenColor;
                }
            }

            copyLayer(zLayer, tempZLayer);
        }
        return xLayer;
    }

    private int[][] denoisifyGreyScaleImage(int[][] observedImage) {
        int[][] hiddenNodes = new int[observedImage.length][observedImage[0].length];
        int[][] tempHidden = new int[observedImage.length][observedImage[0].length];
        copyLayer(hiddenNodes, observedImage);

        while (iterations-- > 0) {
            for (int ii = 0; ii < hiddenNodes.length; ii++) {
                for (int jj = 0; jj < hiddenNodes[ii].length; jj++) {
                    List<Integer> neighbors = getNeighbors(hiddenNodes, ii, jj);
                    int chosenColor = decideColorGreyScale(observedImage[ii][jj], neighbors);
                    tempHidden[ii][jj] = chosenColor;
                }
            }
            copyLayer(hiddenNodes, tempHidden);
        }
        return hiddenNodes;
    }

    private int[][] denoisifyBlackAndWhiteImage(int[][] observedNodes) {
        int[][] hiddenNodes = new int[observedNodes.length][observedNodes[0].length];
        int[][] tempHidden = new int[observedNodes.length][observedNodes[0].length];
        copyLayer(hiddenNodes, observedNodes);

        while (iterations-- > 0) {
            for (int ii = 0; ii < hiddenNodes.length; ii++) {
                for (int jj = 0; jj < hiddenNodes[ii].length; jj++) {
                    List<Integer> neighbors = getNeighbors(hiddenNodes, ii, jj);
                    int chosenColor = decideColorBorW(observedNodes[ii][jj], neighbors);
                    tempHidden[ii][jj] = chosenColor;
                }
            }

            copyLayer(hiddenNodes, tempHidden);
        }
        return hiddenNodes;
    }

    private int decideColorBorW(int observedNode, List<Integer> neighbors) {
        double minEnergy = Double.MAX_VALUE;
        int minEnergyColor = 0;

        for (int color = 0; color <= maxColor; color++) {
            double energy = computeTotalEnergyForBAndWPixel(observedNode, color, neighbors);
            if (energy < minEnergy) {
                minEnergy = energy;
                minEnergyColor = color;
            }
        }
        return minEnergyColor;
    }

    private int decideColorForZPixelBW(
            double observedNode,
            List<Integer> neighbors) {
        double minEnergy = Double.MAX_VALUE;
        int minEnergyColor = 0;

        for (int color = 0; color <= maxColor; color++) {
            double energy = 0;
            for (int neighbor : neighbors) {
                if (neighbor == observedNode) {
                    energy += -omega;
                } else {
                    energy += omega;
                }
            }
            if (energy < minEnergy) {
                minEnergy = energy;
                minEnergyColor = color;
            }
        }

        return minEnergyColor;
    }

    private int decideColorForZPixelGS(
            List<Integer> neighbors) {
        double avg = 0;
        for (int neighbor : neighbors) {
            avg += neighbor;
        }
        return (int) avg / neighbors.size();
    }

    private int decideColorBorWEx(
            int observedNode,
            double hiddenLayer2NodeValue,
            List<Integer> neighbors) {
        double minEnergy = Double.MAX_VALUE;
        int minEnergyColor = -1;

        for (int color = 0; color <= maxColor; color++) {
            double energy = computeTotalEnergyForBAndWPixelEx(observedNode, color, hiddenLayer2NodeValue, neighbors);
            if (energy < minEnergy) {
                minEnergy = energy;
                minEnergyColor = color;
            }
        }
        return minEnergyColor;
    }

    private int decideColorGreyScale(int observedNode, List<Integer> neighbors) {
        int minEnergyColor = 0;
        double minEnergy = computeTotalEnergyForGreyScalePixel(observedNode, 0, neighbors);

        for (int color = 1; color <= maxColor; color++) {
            double energy = computeTotalEnergyForGreyScalePixel(observedNode, color, neighbors);
            if (energy < minEnergy) {
                minEnergy = energy;
                minEnergyColor = color;
            }
        }
        return minEnergyColor;
    }

    private int decideColorGreyScaleEx(
            int observedNode,
            double hiddenLayer2Node,
            List<Integer> neighbors) {
        int minEnergyColor = 0;
        double minEnergy = computeTotalEnergyForGreyScalePixel(observedNode, 0, neighbors);


        for (int color = 1; color <= maxColor; color++) {
            double energy = computeTotalEnergyForGreyScalePixelEx(observedNode, color, hiddenLayer2Node, neighbors);
            if (energy < minEnergy) {
                minEnergyColor = color;
                minEnergy = energy;
            }
        }
        return minEnergyColor;
    }

    private double computeTotalEnergyForGreyScalePixel(
            int observedNode,
            int hiddenNodeValue,
            List<Integer> hiddenNeighbors) {
        double potentialXiXj = 0, potentialXiYi, energy;

        for (Integer neighbor : hiddenNeighbors) {
            potentialXiXj += (Math.log(1 + Math.abs(hiddenNodeValue - neighbor)) - 1) * beta;
        }
        potentialXiYi = (Math.log(1 + Math.abs(hiddenNodeValue - observedNode)) - 1) * eta;

        if (hiddenNeighbors.size() == 0) {
            energy = potentialXiYi;
        } else {
            energy = potentialXiXj + potentialXiYi;
        }
        return energy;
    }

    private double computeTotalEnergyForGreyScalePixelEx(
            int observedNode,
            double hiddenNodeValue,
            double hiddenLayer2NodeValue,
            List<Integer> hiddenNeighbors) {
        double potentialXiXj = 0, potentialXiYi, potentialXiZj, energy;

        for (Integer hiddenNeighbor : hiddenNeighbors) {
            potentialXiXj += (Math.log(1 + Math.abs(hiddenNodeValue - hiddenNeighbor)) - 1) * beta;
        }

        potentialXiYi = ((Math.log(1 + Math.abs(hiddenNodeValue - observedNode))) - 1) * eta;

        potentialXiZj = (Math.log(1 + Math.abs(hiddenNodeValue - hiddenLayer2NodeValue)) - 1) * omega;

        if (hiddenNeighbors.size() == 0) {
            energy = potentialXiYi + potentialXiZj;
        } else {
            energy = potentialXiXj + potentialXiYi + potentialXiZj;
        }
        return energy;


    }

    private double computeTotalEnergyForBAndWPixel(
            int observedNode,
            int hiddenNodeValue,
            List<Integer> hiddenNeighbors) {
        double beta_summationXiXj = 0, eta_XiYi;
        for (Integer neighbor : hiddenNeighbors) {
            double betaLocal;
            betaLocal = neighbor == hiddenNodeValue ? -beta : beta;
            beta_summationXiXj += betaLocal;
        }

        eta_XiYi = hiddenNodeValue == observedNode ? -eta : eta;
        return beta_summationXiXj + eta_XiYi;
    }

    private double computeTotalEnergyForBAndWPixelEx(
            int observedNode,
            int hiddenNodeValue,
            double hiddenLayer2NodeValue,
            List<Integer> hiddenNeighbors) {
        double beta_summationXiXj = 0, eta_XiYi, omega_XiZj;
        for (Integer neighbor : hiddenNeighbors) {
            double betaLocal;
            betaLocal = neighbor == hiddenNodeValue ? -beta : beta;
            beta_summationXiXj += betaLocal;
        }

        eta_XiYi = hiddenNodeValue == observedNode ? -eta : eta;

        omega_XiZj = hiddenNodeValue == hiddenLayer2NodeValue ? -omega : omega;

        return beta_summationXiXj + eta_XiYi + omega_XiZj;
    }

    private List<Integer> getNeighbors(int[][] hiddenNodes, int rowIdx, int colIdx) {
        List<Integer> neighbors = new ArrayList<Integer>();
        if (colIdx != 0) {
            neighbors.add(hiddenNodes[rowIdx][colIdx - 1]);  // left neighbor
        }
        if (colIdx != hiddenNodes[0].length - 1) {
            neighbors.add(hiddenNodes[rowIdx][colIdx + 1]);  // right neighbor
        }

        if (rowIdx != 0) {
            neighbors.add(hiddenNodes[rowIdx - 1][colIdx]);  // top neighbor
        }

        if (rowIdx != hiddenNodes.length - 1) {
            neighbors.add(hiddenNodes[rowIdx + 1][colIdx]);  // bottom neighbor
        }
        return neighbors;
    }

    private List<Integer> getNeighborsOfZi(int[][] hiddenLayerX, int rowIdx, int colIdx) {
        List<Integer> neighbors = new ArrayList<Integer>();

        int rowLimit = Math.min(rowIdx * numK + numK, hiddenLayerX.length);

        int colLimit = Math.min(colIdx * numK + numK, hiddenLayerX[0].length);

        for (int i = rowIdx * numK; i < rowLimit; i++) {
            for (int j = colIdx * numK; j < colLimit; j++) {
                neighbors.add(hiddenLayerX[i][j]);
            }
        }
        return neighbors;
    }

    private void copyLayer(int[][] hiddenNodes, int[][] observedNodes) {
        for (int ii = 0; ii < observedNodes.length; ii++) {
            System.arraycopy(observedNodes[ii], 0, hiddenNodes[ii], 0, observedNodes[ii].length);
        }
    }
}
*/
